#
# This file is part of Astarte.
#
# Copyright 2025 SECO Mind Srl
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

defmodule Astarte.AppEngine.API.Device.Aliases do
  alias Astarte.DataAccess.Astarte.Realm
  alias Astarte.DataAccess.Realms.Device
  alias Astarte.DataAccess.Realms.Name
  alias Ecto.Changeset

  alias Astarte.DataAccess.Repo

  import Ecto.Query

  require Logger

  defstruct to_update: [], to_delete: []

  @type input :: %{alias_tag => alias_value} | [alias]
  @type alias_tag :: String.t()
  @type alias_value :: String.t()
  @type alias :: {alias_tag, alias_value}
  @type t :: %__MODULE__{
          to_update: [alias],
          to_delete: [alias_tag]
        }

  @spec validate(input() | nil, String.t(), Device.t()) :: {:ok, t()} | term()
  def validate(nil, _, _), do: {:ok, %__MODULE__{to_delete: [], to_update: []}}

  def validate(aliases, realm_name, device) do
    with :ok <- validate_format(aliases) do
      {to_delete, to_update} = aliases |> Enum.split_with(fn {_key, value} -> is_nil(value) end)
      to_delete = to_delete |> Enum.map(fn {tag, nil} -> tag end)
      state = %__MODULE__{to_delete: to_delete, to_update: to_update}

      with :ok <- validate_device_ownership(state, realm_name, device) do
        {:ok, state}
      end
    end
  end

  @spec apply(Changeset.t(), t()) :: Changeset.t()
  def apply(changeset, aliases) do
    %__MODULE__{to_delete: to_delete, to_update: to_update} = aliases

    changeset
    |> apply_delete(to_delete)
    |> apply_update(to_update)
  end

  @spec validate_format(input()) :: :ok | {:error, :invalid_alias}
  defp validate_format(aliases) do
    Enum.find_value(aliases, :ok, fn
      {_tag, ""} ->
        :invalid_value

      {"", _value} ->
        :invalid_tag

      _valid_format_tag ->
        false
    end)
    |> case do
      :ok ->
        :ok

      :invalid_tag ->
        Logger.warning("Alias key cannot be an empty string.", tag: :invalid_alias_empty_key)
        {:error, :invalid_alias}

      :invalid_value ->
        Logger.warning("Alias value cannot be an empty string.", tag: :invalid_alias_empty_value)
        {:error, :invalid_alias}
    end
  end

  @spec validate_device_ownership(t(), String.t(), Device.t()) :: :ok
  defp validate_device_ownership(aliases, realm_name, device) do
    keyspace = Realm.keyspace_name(realm_name)

    %__MODULE__{to_delete: to_delete, to_update: to_update} = aliases

    to_delete = device.aliases |> Map.take(to_delete) |> Enum.map(fn {_tag, value} -> value end)
    to_update = to_update |> Enum.map(fn {_tag, value} -> value end)

    chunked_aliases = Enum.concat(to_delete, to_update) |> Enum.chunk_every(99)

    results =
      for alias_chunk <- chunked_aliases do
        from(n in Name, where: n.object_type == 1 and n.object_name in ^alias_chunk)
        |> Repo.all(prefix: keyspace)
      end
      |> List.flatten()

    invalid_name =
      results |> Enum.find(fn name -> name.object_uuid != device.device_id end)

    if is_nil(invalid_name) do
      :ok
    else
      existing_aliases =
        Enum.find(device.aliases, fn {_tag, value} -> value == invalid_name.object_name end)

      inconsistent? = !is_nil(existing_aliases)

      if inconsistent? do
        {invalid_tag, _value} = existing_aliases

        Logger.error("Inconsistent alias for #{invalid_tag}.",
          device_id: device.device_id,
          tag: "inconsistent_alias"
        )

        {:error, :database_error}
      else
        {:error, :alias_already_in_use}
      end
    end
  end

  @spec generate_batch_queries(t(), String.t(), Device.t()) :: [{String.t(), list()}]
  def generate_batch_queries(aliases, keyspace, device) do
    %__MODULE__{to_delete: to_delete, to_update: to_update} = aliases

    {update_tags, update_values} = Enum.unzip(to_update)

    all_tags = to_delete ++ update_tags

    tags_to_delete =
      device.aliases
      |> Enum.filter(fn {tag, _value} -> tag in all_tags end)

    # We delete both aliases we mean to delete, and also existing aliases we want to update
    # as the name is part of the primary key for the names table.
    # Queries are chunked to avoid hitting scylla's `max_clustering_key_restrictions_per_query`
    delete_queries =
      tags_to_delete
      |> Enum.map(fn {_tag, value} -> value end)
      |> Enum.chunk_every(99)
      |> Enum.map(fn alias_chunk ->
        query =
          from n in Name,
            prefix: ^keyspace,
            where: n.object_type == 1 and n.object_name in ^alias_chunk

        Repo.to_sql(:delete_all, query)
      end)

    insert_queries =
      update_values
      |> Enum.map(&update_batch_query(keyspace, device.device_id, &1))

    delete_queries ++ insert_queries
  end

  defp update_batch_query(keyspace, device_id, value) do
    names_table = %Name{}.__meta__.source

    query =
      "INSERT INTO #{keyspace}.#{names_table} (object_type, object_name, object_uuid) VALUES (1, ?, ?)"

    params = [value, device_id]
    {query, params}
  end

  @spec apply_delete(Changeset.t(), [alias]) :: Changeset.t()
  defp apply_delete(%Changeset{valid?: false} = changeset, _delete_aliases),
    do: changeset

  defp apply_delete(changeset, delete_aliases) when length(delete_aliases) == 0,
    do: changeset

  defp apply_delete(changeset, delete_aliases) do
    aliases = changeset |> Changeset.fetch_field!(:aliases)

    delete_tags = delete_aliases |> MapSet.new()

    device_aliases = aliases |> Map.keys() |> MapSet.new()

    if MapSet.subset?(delete_tags, device_aliases) do
      aliases = aliases |> Map.drop(delete_aliases)

      changeset
      |> Changeset.put_change(:aliases, aliases)
    else
      Changeset.add_error(changeset, :aliases, "", reason: :alias_tag_not_found)
    end
  end

  @spec apply_update(Changeset.t(), [alias]) :: Changeset.t()
  defp apply_update(%Changeset{valid?: false} = changeset, _update_aliases),
    do: changeset

  defp apply_update(changeset, update_aliases) when length(update_aliases) == 0,
    do: changeset

  defp apply_update(changeset, update_aliases) do
    aliases =
      changeset |> Changeset.fetch_field!(:aliases)

    aliases = Map.merge(aliases, Map.new(update_aliases))

    Changeset.put_change(changeset, :aliases, aliases)
  end
end
