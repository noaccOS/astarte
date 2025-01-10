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
  alias Astarte.Core.Device, as: CoreDevice
  alias Astarte.DataAccess.Astarte.Realm
  alias Astarte.DataAccess.Realms.Device
  alias Astarte.DataAccess.Realms.Name
  alias Ecto.Changeset

  alias Astarte.DataAccess.Repo

  import Ecto.Query

  require Logger

  defstruct to_update: %{}, to_delete: %{}

  @type input :: %{alias_tag => alias_value} | [alias]
  @type alias_tag :: String.t()
  @type alias_value :: String.t()
  @type alias :: {alias_tag, alias_value}
  @type t :: %__MODULE__{
          to_update: [alias],
          to_delete: [alias]
        }

  @spec validate(input() | nil, String.t(), CoreDevice.device_id()) :: {:ok, t()} | term()
  def validate(nil, _, _), do: {:ok, %__MODULE__{to_delete: [], to_update: []}}

  def validate(aliases, realm_name, device_id) do
    with :ok <- validate_format(aliases),
         :ok <- validate_device_ownership(aliases, realm_name, device_id) do
      {to_delete, to_update} =
        aliases
        |> Enum.split_with(fn {_key, value} -> is_nil(value) end)

      {:ok, %__MODULE__{to_delete: to_delete, to_update: to_update}}
    end
  end

  @spec apply(Changeset.t(), t(), String.t()) :: Changeset.t()
  def apply(changeset, aliases, realm_name) do
    %__MODULE__{to_delete: to_delete, to_update: to_update} = aliases
    device_id = changeset |> Changeset.fetch_field!(:device_id)

    changeset
    # |> Ecto.Changeset.prepare_changes(fn changeset ->
    # We validate device ownership in prepare_changes to have it happen in the same transaction
    # as the updates.
    # Prepares are executed in order,
    # so this check will happen before other prepares defined in update and delete
    # validate_device_ownership(changeset, aliases, realm_name, device_id)
    #   changeset
    # end)
    |> apply_delete(to_delete, realm_name)
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

  @spec validate_device_ownership(input(), String.t(), CoreDevice.device_id()) :: :ok
  defp validate_device_ownership(aliases, realm_name, device_id) do
    keyspace = Realm.keyspace_name(realm_name)
    chunked_aliases = aliases |> Enum.map(fn {tag, _value} -> tag end) |> Enum.chunk_every(99)

    results =
      for alias_chunk <- chunked_aliases do
        from(n in Name, where: n.object_type == 1 and n.object_name in ^alias_chunk)
        |> Repo.all(prefix: keyspace)
      end

    invalid_alias =
      results |> Enum.find_value(&Enum.find(&1, fn name -> name.object_uuid != device_id end))

    if is_nil(invalid_alias) do
      :ok
    else
      Logger.error("Inconsistent alias for #{invalid_alias.alias_tag}.",
        device_id: device_id,
        tag: "inconsistent_alias"
      )

      {:error, :database_error}
    end
  end

  @spec generate_batch_queries(t(), String.t(), Device.t()) :: [{String.t(), list()}]
  def generate_batch_queries(aliases, keyspace, device_id) do
    %__MODULE__{to_delete: to_delete, to_update: to_update} = aliases

    delete_queries =
      to_delete
      |> Enum.map(fn {_tag, value} -> value end)
      |> Enum.chunk_every(99)
      |> Enum.map(fn alias_chunk ->
        query =
          from n in Name,
            prefix: ^keyspace,
            where: n.object_type == 1 and n.object_name in ^alias_chunk

        Repo.to_sql(:delete_all, query)
      end)

    update_queries =
      to_update
      |> Enum.map(fn {_tag, value} -> value end)
      |> Enum.chunk_every(99)
      |> Enum.map(fn alias_chunk ->
        query =
          from n in Name,
            prefix: ^keyspace,
            update: [
              set: [
                object_type: 1,
                object_uuid: ^device_id
              ]
            ],
            where: n.object_name in ^alias_chunk

        Repo.to_sql(:update_all, query)
      end)

    delete_queries ++ update_queries
  end

  @spec apply_delete(Changeset.t(), [alias], String.t()) :: Changeset.t()
  defp apply_delete(%Changeset{valid?: false} = changeset, _delete_aliases, _realm_name),
    do: changeset

  defp apply_delete(changeset, delete_aliases, _realm_name) when length(delete_aliases) == 0,
    do: changeset

  defp apply_delete(changeset, delete_aliases, realm_name) do
    keyspace = Realm.keyspace_name(realm_name)
    aliases = changeset |> Changeset.fetch_field!(:aliases)

    {delete_tags, delete_values} = Enum.unzip(delete_aliases)
    delete_tags = delete_tags |> MapSet.new()

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
  defp apply_update(%Changeset{valid?: false} = changeset, _update_aliases), do: changeset

  defp apply_update(changeset, update_aliases) when length(update_aliases) == 0, do: changeset

  defp apply_update(changeset, update_aliases) do
    aliases =
      changeset |> Changeset.fetch_field!(:aliases)

    aliases = Map.merge(aliases, Map.new(update_aliases))

    # Works because inserts in CQL are upserts
    Changeset.put_change(changeset, :aliases, aliases)
    # |> Changeset.prepare_changes(fn changeset ->
    #   device_id = changeset |> Changeset.fetch_field!(:device_id)

    #   entries =
    #     for {_alias_tag, alias_value} <- update_aliases do
    #       %{object_name: alias_value, object_type: 1, object_uuid: device_id}
    #     end

    #   Repo.insert_all(Name, entries)
    # end)
  end
end
