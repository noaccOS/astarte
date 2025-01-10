#
# This file is part of Astarte.
#
# Copyright 2017-2023 Ispirata Srl
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

defmodule Astarte.AppEngine.API.Device do
  @moduledoc """
  The Device context.
  """

  alias Astarte.AppEngine.API.DataTransmitter
  alias Astarte.AppEngine.API.Device.Aliases
  alias Astarte.AppEngine.API.Device.AstarteValue
  alias Astarte.AppEngine.API.Device.Attributes
  alias Astarte.AppEngine.API.Device.DevicesList
  alias Astarte.AppEngine.API.Device.DevicesListOptions
  alias Astarte.AppEngine.API.Device.DeviceStatus
  alias Astarte.AppEngine.API.Device.InterfaceInfo
  alias Astarte.AppEngine.API.Device.InterfaceValue
  alias Astarte.AppEngine.API.Device.InterfaceValues
  alias Astarte.AppEngine.API.Device.InterfaceValuesOptions
  alias Astarte.AppEngine.API.Device.MapTree
  alias Astarte.AppEngine.API.Device.Queries
  alias Astarte.Core.CQLUtils
  alias Astarte.Core.Device
  alias Astarte.Core.InterfaceDescriptor
  alias Astarte.Core.Mapping
  alias Astarte.Core.Mapping.EndpointsAutomaton
  alias Astarte.Core.Mapping.ValueType
  alias Astarte.DataAccess.Astarte.Realm
  alias Astarte.DataAccess.Database
  alias Astarte.DataAccess.Device, as: DeviceQueries
  alias Astarte.DataAccess.Interface, as: InterfaceQueries
  alias Astarte.DataAccess.Mappings
  alias Astarte.DataAccess.Realms.IndividualProperty, as: DatabaseIndividualProperty
  alias Astarte.DataAccess.Realms.Endpoint, as: DatabaseEndpoint
  alias Astarte.DataAccess.Realms.Device, as: DatabaseDevice
  alias Astarte.DataAccess.Realms.Name, as: DatabaseName
  alias Astarte.DataAccess.Repo
  alias Ecto.Changeset

  require Logger

  import Ecto.Query

  # def from_gleam(a, b) do
  #   :device.sum(a, b)
  # end

  def list_devices!(realm_name, params) do
    changeset = DevicesListOptions.changeset(%DevicesListOptions{}, params)

    with {:ok, opts} <- Changeset.apply_action(changeset, :insert) do
      with_details? = opts.details

      devices =
        Queries.retrieve_devices_list(
          realm_name,
          opts.limit,
          with_details?,
          opts.from_token
        )
        |> Repo.all()

      devices_info =
        if with_details? do
          devices |> Enum.map(fn device -> DeviceStatus.from_device(device, realm_name) end)
        else
          devices
          |> Enum.map(fn device ->
            Device.encode_device_id(device.device_id)
          end)
        end

      device_list =
        if Enum.count(devices) < opts.limit do
          %DevicesList{devices: devices_info}
        else
          token = devices |> List.last() |> Map.fetch!("token")
          %DevicesList{devices: devices_info, last_token: token}
        end

      {:ok, device_list}
    end
  end

  @doc """
  Returns a DeviceStatus struct which represents device status.
  Device status returns information such as connected, last_connection and last_disconnection.
  """
  def get_device_status!(realm_name, encoded_device_id) do
    with {:ok, device_id} <- Device.decode_device_id(encoded_device_id) do
      retrieve_device_status(realm_name, device_id)
    end
  end

  def merge_device_status(realm_name, encoded_device_id, device_status_merge) do
    keyspace = Realm.keyspace_name(realm_name)

    with {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
         {:ok, device} <-
           Repo.fetch(DatabaseDevice, device_id, prefix: keyspace, error: :device_not_found),
         {:ok, device} <- update_device_status(realm_name, device, device_status_merge) do
      {:ok, DeviceStatus.from_device(device, realm_name)}
    end
  end

  defp update_device_status(realm_name, device, device_status_merge) do
    keyspace = Realm.keyspace_name(realm_name)
    aliases = device_status_merge["aliases"]
    attributes = device_status_merge["attributes"]
    device_id = device.device_id

    with {:ok, aliases} <- Aliases.validate(aliases, realm_name, device_id),
         {:ok, attributes} <- Attributes.validate(attributes) do
      params =
        case Map.fetch(device_status_merge, "credentials_inhibited") do
          {:ok, credentials_inhibited} -> %{inhibit_credentials_request: credentials_inhibited}
          :error -> %{}
        end

      changeset =
        device
        |> Changeset.cast(params, [:inhibit_credentials_request])
        |> Aliases.apply(aliases, realm_name)
        |> Attributes.apply(attributes)

      with {:ok, device} <- Changeset.apply_action(changeset, :update) do
        execute_merge_queries(keyspace, device_id, changeset, aliases)
        {:ok, device}
      end
    end
  end

  defp execute_merge_queries(_keyspace, _device_id, %Changeset{changes: changes}, _aliases)
       when map_size(changes) == 0,
       do: :ok

  defp execute_merge_queries(keyspace, device_id, changeset, aliases) do
    changes = changeset.changes |> Keyword.new()

    device_query =
      from d in DatabaseDevice,
        prefix: ^keyspace,
        where: d.device_id == ^device_id,
        update: [set: ^changes]

    device_query = Repo.to_sql(:update_all, device_query)
    aliases_queries = Aliases.generate_batch_queries(aliases, keyspace, device_id)

    queries = [device_query | aliases_queries]

    Exandra.execute_batch(Repo, %Exandra.Batch{queries: queries}, consistency: :each_quorum)
  end

  defp try_extract_reason(changeset) do
    # if there is a custom error, return it: it was created by Aliases.apply or Attributes.apply
    Enum.find_value(changeset.errors, changeset, fn
      {:aliases, {_message, [reason: reason]}} -> reason
      {:attributes, {_message, [reason: reason]}} -> reason
      _ -> false
    end)
  end

  defp validate_attributes(device_status_merge) do
    attributes =
      device_status_merge
      |> Map.get("attributes")
      |> Kernel.||([])

    try do
      # first, check for invalid values and fail fast if there is any
      for {key, _value} <- attributes, key == "" do
        Logger.warning("Attribute key cannot be an empty string.",
          tag: :invalid_attribute_empty_key
        )

        throw({:error, :invalid_attribute})
      end

      {delete, update} =
        attributes
        |> Enum.split_with(fn {_key, value} -> is_nil(value) end)

      {:ok, %{delete: delete, update: update}}
    catch
      error -> error
    end
  end

  defp update_attributes(realm_name, device, attributes) do
    Enum.reduce_while(attributes, {:ok, device}, fn
      {"", _attribute_value}, _acc ->
        Logger.warning("Attribute key cannot be an empty string.",
          tag: :invalid_attribute_empty_key
        )

        {:halt, {:error, :invalid_attributes}}

      {attribute_key, nil}, {:ok, device} ->
        case delete_attribute(realm_name, device, attribute_key) do
          {:ok, device} ->
            {:cont, {:ok, device}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end

      {attribute_key, attribute_value}, {:ok, device} ->
        case insert_attribute(realm_name, device, attribute_key, attribute_value) do
          :ok ->
            {:cont, :ok}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
    end)
  end

  def insert_attribute(realm_name, device, attribute_key, attribute_value) do
    keyspace = Realm.keyspace_name(realm_name)
    attributes = device.attributes |> Map.put(attribute_key, attribute_value)

    device
    |> Ecto.Changeset.change(attributes: attributes)
    |> Repo.update!(prefix: keyspace, consistency: :each_quorum)
  end

  def delete_attribute(realm_name, device, attribute_key) do
    case Map.pop(device.attributes, attribute_key) do
      {nil, _attributes} ->
        {:error, :attribute_key_not_found}

      {_attribute_value, attributes} ->
        keyspace = Realm.keyspace_name(realm_name)

        device =
          device
          |> Ecto.Changeset.change(attributes: attributes)
          |> Repo.update!(prefix: keyspace, consistency: :each_quorum)

        {:ok, device}
    end
  end

  defp update_aliases(realm_name, device, aliases) do
    Enum.reduce_while(aliases, {:ok, device}, fn
      {_alias_key, ""}, _acc ->
        Logger.warning("Alias value cannot be an empty string.", tag: :invalid_alias_empty_value)
        {:halt, {:error, :invalid_alias}}

      {"", _alias_value}, _acc ->
        Logger.warning("Alias key cannot be an empty string.", tag: :invalid_alias_empty_key)
        {:halt, {:error, :invalid_alias}}

      {alias_key, nil}, {:ok, device} ->
        case delete_alias(realm_name, device, alias_key) do
          {:ok, device} -> {:cont, {:ok, device}}
          {:error, reason} -> {:halt, {:error, reason}}
        end

      {alias_key, alias_value}, {:ok, device} ->
        case insert_alias(realm_name, device, alias_key, alias_value) do
          {:ok, device} ->
            {:cont, {:ok, device}}

          {:error, reason} ->
            {:halt, {:error, reason}}
        end
    end)
  end

  def insert_alias(realm_name, device, alias_tag, alias_value) do
    keyspace = Realm.keyspace_name(realm_name)
    device_id = device.device_id

    insert_alias_to_names =
      "INSERT INTO #{keyspace}.names (object_name, object_type, object_uuid) VALUES (?, 1, ?)"

    insert_alias_to_names = {insert_alias_to_names, [alias_value, device_id]}

    insert_alias_to_device = "UPDATE #{keyspace}.devices SET aliases[?] = ? WHERE device_id = ?"
    insert_alias_to_device = {insert_alias_to_device, [alias_tag, alias_value, device_id]}

    insert_batch =
      %Exandra.Batch{queries: [insert_alias_to_device, insert_alias_to_names]}

    with :ok <- alias_available?(realm_name, alias_value),
         {:ok, device} <- delete_alias_if_exists(realm_name, device, alias_tag),
         :ok <- Exandra.execute_batch(Repo, insert_batch, conosistency: :each_quorum) do
      device = put_in(device, [:aliases, alias_tag], alias_value)
      {:ok, device}
    end
  end

  defp alias_available?(realm_name, alias_value) do
    case device_alias_to_device_id(realm_name, alias_value) do
      {:error, :device_not_found} -> :ok
      {:ok, _} -> {:error, :alias_already_in_use}
    end
  end

  defp delete_alias_if_exists(realm_name, device, alias_tag) do
    case delete_alias(realm_name, device, alias_tag) do
      {:ok, device} ->
        {:ok, device}

      {:error, :alias_tag_not_found} ->
        {:ok, device}

      not_ok ->
        not_ok
    end
  end

  def delete_alias(realm_name, device, alias_tag) do
    keyspace = Realm.keyspace_name(realm_name)
    aliases = device.aliases
    device_id = device.device_id

    with {:ok, alias_value} <- find_alias(aliases, alias_tag),
         :ok <- check_alias_ownership(realm_name, device_id, alias_tag, alias_value) do
      delete_alias_from_device = "DELETE aliases[?] FROM #{keyspace}.devices WHERE device_id = ?"
      delete_alias_from_device = {delete_alias_from_device, [alias_tag, device_id]}

      delete_alias_from_names =
        "DELETE FROM #{keyspace}.names WHERE object_type = 1 and object_name = ?"

      delete_alias_from_names = {delete_alias_from_names, [alias_value]}

      # batches are second class citizens and require queries as strings
      delete_batch = %Exandra.Batch{
        queries: [
          delete_alias_from_device,
          delete_alias_from_names
        ]
      }

      with :ok <- Exandra.execute_batch(Repo, delete_batch, consistency: :each_quorum) do
        {_alias_value, device} = pop_in(device, [:aliases, alias_tag])
        {:ok, device}
      end
    end
  end

  defp check_alias_ownership(realm_name, device_id, alias_tag, alias_value) do
    case device_alias_to_device_id(realm_name, alias_value) do
      {:ok, ^device_id} ->
        :ok

      _ ->
        Logger.error("Inconsistent alias for #{alias_tag}.",
          device_id: device_id,
          tag: "inconsistent_alias"
        )

        {:error, :database_error}
    end
  end

  defp find_alias(device_aliases, alias) do
    with :error <- Map.fetch(device_aliases, alias) do
      {:error, :alias_tag_not_found}
    end
  end

  defp merge_data(old_data, new_data) when is_map(old_data) and is_map(new_data) do
    Map.merge(old_data, new_data)
    |> Enum.reject(fn {_, v} -> v == nil end)
    |> Enum.into(%{})
  end

  defp change_credentials_inhibited(_client, _device_id, nil) do
    :ok
  end

  defp change_credentials_inhibited(client, device_id, credentials_inhibited)
       when is_boolean(credentials_inhibited) do
    Queries.set_inhibit_credentials_request(client, device_id, credentials_inhibited)
  end

  @doc """
  Returns the list of interfaces.
  """
  def list_interfaces(realm_name, encoded_device_id) do
    device_introspection = Queries.retrieve_interfaces_list(realm_name)

    with {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
         {:ok, device} <- Repo.fetch(device_introspection, device_id, error: :device_not_found) do
      interface_names = device.introspection |> Map.keys()
      {:ok, interface_names}
    end
  end

  @doc """
  Gets all values set on a certain interface.
  This function handles all GET requests on /{realm_name}/devices/{device_id}/interfaces/{interface}
  """
  def get_interface_values!(realm_name, encoded_device_id, interface, params) do
    changeset = InterfaceValuesOptions.changeset(%InterfaceValuesOptions{}, params)

    with {:ok, options} <- Changeset.apply_action(changeset, :insert),
         {:ok, client} <- Database.connect(realm: realm_name),
         {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
         {:ok, major_version} <-
           DeviceQueries.interface_version(realm_name, device_id, interface),
         {:ok, interface_row} <-
           InterfaceQueries.retrieve_interface_row(realm_name, interface, major_version) do
      do_get_interface_values!(
        realm_name,
        client,
        device_id,
        interface_row.aggregation,
        interface_row,
        options
      )
    end
  end

  @doc """
  Gets a single interface_values.

  Raises if the Interface values does not exist.
  """
  def get_interface_values!(realm_name, encoded_device_id, interface, no_prefix_path, params) do
    changeset = InterfaceValuesOptions.changeset(%InterfaceValuesOptions{}, params)

    with {:ok, options} <- Changeset.apply_action(changeset, :insert),
         {:ok, client} <- Database.connect(realm: realm_name),
         {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
         {:ok, major_version} <-
           DeviceQueries.interface_version(realm_name, device_id, interface),
         {:ok, interface_row} <-
           InterfaceQueries.retrieve_interface_row(realm_name, interface, major_version),
         path <- "/" <> no_prefix_path,
         {:ok, interface_descriptor} <- InterfaceDescriptor.from_db_result(interface_row),
         {:ok, endpoint_ids} <-
           get_endpoint_ids(interface_descriptor.automaton, path, allow_guess: true) do
      do_get_interface_values!(
        realm_name,
        client,
        device_id,
        interface_row.aggregation,
        interface_row.type,
        interface_row,
        endpoint_ids,
        path,
        options
      )
    end
  end

  defp update_individual_interface_values(
         realm_name,
         device_id,
         interface_descriptor,
         path,
         raw_value
       ) do
    with {:ok, [endpoint_id]} <- get_endpoint_ids(interface_descriptor.automaton, path),
         mapping =
           Queries.retrieve_mapping(realm_name)
           |> Repo.get_by!(%{
             interface_id: interface_descriptor.interface_id,
             endpoint_id: endpoint_id
           }),
         {:ok, value} <- InterfaceValue.cast_value(mapping.value_type, raw_value),
         :ok <- validate_value_type(mapping.value_type, value),
         wrapped_value = wrap_to_bson_struct(mapping.value_type, value),
         interface_type = interface_descriptor.type,
         reliability = mapping.reliability,
         publish_opts = build_publish_opts(interface_type, reliability),
         interface_name = interface_descriptor.name,
         :ok <-
           ensure_publish(
             realm_name,
             device_id,
             interface_name,
             path,
             wrapped_value,
             publish_opts
           ) do
      realm_max_ttl = Queries.datastream_maximum_storage_retention(realm_name) |> Repo.one()

      now =
        DateTime.utc_now()

      db_max_ttl =
        if mapping.database_retention_policy == :use_ttl do
          min(realm_max_ttl, mapping.database_retention_ttl)
        else
          realm_max_ttl
        end

      opts = [ttl: db_max_ttl]

      insert_value_into_db(
        realm_name,
        device_id,
        interface_descriptor,
        endpoint_id,
        mapping,
        path,
        value,
        now,
        opts
      )

      if interface_descriptor.type == :datastream do
        insert_path_into_db(
          realm_name,
          device_id,
          interface_descriptor,
          endpoint_id,
          path,
          now,
          opts
        )
      end

      {:ok,
       %InterfaceValues{
         data: raw_value
       }}
    else
      {:error, :endpoint_guess_not_allowed} ->
        _ = Logger.warning("Incomplete path not allowed.", tag: "endpoint_guess_not_allowed")
        {:error, :read_only_resource}

      {:error, :unexpected_value_type, expected: value_type} ->
        _ = Logger.warning("Unexpected value type.", tag: "unexpected_value_type")
        {:error, :unexpected_value_type, expected: value_type}

      {:error, reason} ->
        _ = Logger.warning("Error while writing to interface.", tag: "write_to_device_error")
        {:error, reason}
    end
  end

  defp path_or_endpoint_depth(path) when is_binary(path) do
    String.split(path, "/", trim: true)
    |> length()
  end

  defp resolve_object_aggregation_path(
         path,
         %InterfaceDescriptor{aggregation: :object} = interface_descriptor,
         mappings
       ) do
    mappings =
      Map.new(mappings, fn mapping ->
        {mapping.endpoint_id, mapping}
      end)

    with {:guessed, guessed_endpoints} <-
           EndpointsAutomaton.resolve_path(path, interface_descriptor.automaton),
         :ok <- check_object_aggregation_prefix(path, guessed_endpoints, mappings) do
      endpoint_id =
        CQLUtils.endpoint_id(
          interface_descriptor.name,
          interface_descriptor.major_version,
          ""
        )

      {:ok, %Mapping{endpoint_id: endpoint_id}}
    else
      {:ok, _endpoint_id} ->
        # This is invalid here, publish doesn't happen on endpoints in object aggregated interfaces
        Logger.warning(
          "Tried to publish on endpoint #{inspect(path)} for object aggregated " <>
            "interface #{inspect(interface_descriptor.name)}. You should publish on " <>
            "the common prefix",
          tag: "invalid_path"
        )

        {:error, :mapping_not_found}

      {:error, :not_found} ->
        Logger.warning(
          "Tried to publish on invalid path #{inspect(path)} for object aggregated " <>
            "interface #{inspect(interface_descriptor.name)}",
          tag: "invalid_path"
        )

        {:error, :mapping_not_found}

      {:error, :invalid_object_aggregation_path} ->
        Logger.warning(
          "Tried to publish on invalid path #{inspect(path)} for object aggregated " <>
            "interface #{inspect(interface_descriptor.name)}",
          tag: "invalid_path"
        )

        {:error, :mapping_not_found}
    end
  end

  defp check_object_aggregation_prefix(path, guessed_endpoints, mappings) do
    received_path_depth = path_or_endpoint_depth(path)

    Enum.reduce_while(guessed_endpoints, :ok, fn
      endpoint_id, _acc ->
        with {:ok, %Mapping{endpoint: endpoint}} <- Map.fetch(mappings, endpoint_id),
             endpoint_depth when received_path_depth == endpoint_depth - 1 <-
               path_or_endpoint_depth(endpoint) do
          {:cont, :ok}
        else
          _ ->
            {:halt, {:error, :invalid_object_aggregation_path}}
        end
    end)
  end

  defp object_retention([first | _rest] = _mappings) do
    if first.database_retention_policy == :no_ttl do
      nil
    else
      first.database_retention_ttl
    end
  end

  defp update_object_interface_values(
         realm_name,
         device_id,
         interface_descriptor,
         path,
         raw_value
       ) do
    now =
      DateTime.utc_now()

    with {:ok, mappings} <-
           Mappings.fetch_interface_mappings(
             realm_name,
             interface_descriptor.interface_id
           ),
         {:ok, endpoint} <-
           resolve_object_aggregation_path(path, interface_descriptor, mappings),
         endpoint_id <- endpoint.endpoint_id,
         expected_types <- extract_expected_types(mappings),
         {:ok, value} <- InterfaceValue.cast_value(expected_types, raw_value),
         :ok <- validate_value_type(expected_types, value),
         wrapped_value = wrap_to_bson_struct(expected_types, value),
         reliability = extract_aggregate_reliability(mappings),
         interface_type = interface_descriptor.type,
         publish_opts = build_publish_opts(interface_type, reliability),
         interface_name = interface_descriptor.name,
         :ok <-
           ensure_publish(
             realm_name,
             device_id,
             interface_name,
             path,
             wrapped_value,
             publish_opts
           ) do
      realm_max_ttl = Queries.datastream_maximum_storage_retention(realm_name) |> Repo.one()
      db_max_ttl = min(realm_max_ttl, object_retention(mappings))

      opts =
        case db_max_ttl do
          nil ->
            []

          _ ->
            [ttl: db_max_ttl]
        end

      insert_value_into_db(
        realm_name,
        device_id,
        interface_descriptor,
        nil,
        nil,
        path,
        value,
        now,
        opts
      )

      insert_path_into_db(
        realm_name,
        device_id,
        interface_descriptor,
        endpoint_id,
        path,
        now,
        opts
      )

      {:ok,
       %InterfaceValues{
         data: raw_value
       }}
    else
      {:error, :unexpected_value_type, expected: value_type} ->
        Logger.warning("Unexpected value type.", tag: "unexpected_value_type")
        {:error, :unexpected_value_type, expected: value_type}

      {:error, :invalid_object_aggregation_path} ->
        Logger.warning("Error while trying to publish on path for object aggregated interface.",
          tag: "invalid_object_aggregation_path"
        )

        {:error, :invalid_object_aggregation_path}

      {:error, :mapping_not_found} ->
        {:error, :mapping_not_found}

      {:error, :database_error} ->
        Logger.warning("Error while trying to retrieve ttl.", tag: "database_error")
        {:error, :database_error}

      {:error, reason} ->
        Logger.warning(
          "Unhandled error while updating object interface values: #{inspect(reason)}."
        )

        {:error, reason}
    end
  end

  def update_interface_values(
        realm_name,
        encoded_device_id,
        interface,
        no_prefix_path,
        raw_value,
        _params
      ) do
    with {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
         {:ok, major_version} <-
           DeviceQueries.interface_version(realm_name, device_id, interface),
         {:ok, interface_row} <-
           InterfaceQueries.retrieve_interface_row(realm_name, interface, major_version),
         {:ok, interface_descriptor} <- InterfaceDescriptor.from_db_result(interface_row),
         {:ownership, :server} <- {:ownership, interface_descriptor.ownership},
         path <- "/" <> no_prefix_path do
      if interface_descriptor.aggregation == :individual do
        update_individual_interface_values(
          realm_name,
          device_id,
          interface_descriptor,
          path,
          raw_value
        )
      else
        update_object_interface_values(
          realm_name,
          device_id,
          interface_descriptor,
          path,
          raw_value
        )
      end
    else
      {:ownership, :device} ->
        _ = Logger.warning("Invalid write (device owned).", tag: "cannot_write_to_device_owned")
        {:error, :cannot_write_to_device_owned}

      {:error, reason} ->
        _ = Logger.warning("Error while writing to interface.", tag: "write_to_device_error")
        {:error, reason}
    end
  end

  defp extract_expected_types(mappings) do
    Enum.into(mappings, %{}, fn mapping ->
      expected_key =
        mapping.endpoint
        |> String.split("/")
        |> List.last()

      {expected_key, mapping.value_type}
    end)
  end

  defp extract_aggregate_reliability([mapping | _rest] = _mappings) do
    # Extract the reliability from the first mapping since it's
    # the same for all mappings in object aggregated interfaces
    mapping.reliability
  end

  defp build_publish_opts(:properties, _reliability) do
    [type: :properties]
  end

  defp build_publish_opts(:datastream, reliability) do
    [type: :datastream, reliability: reliability]
  end

  defp ensure_publish(realm, device_id, interface, path, value, opts) do
    with {:ok, %{local_matches: local_matches, remote_matches: remote_matches}} <-
           publish_data(realm, device_id, interface, path, value, opts),
         :ok <- ensure_publish_reliability(local_matches, remote_matches, opts) do
      :ok
    end
  end

  defp publish_data(realm, device_id, interface, path, value, opts) do
    case Keyword.fetch!(opts, :type) do
      :properties ->
        DataTransmitter.set_property(
          realm,
          device_id,
          interface,
          path,
          value
        )

      :datastream ->
        qos =
          Keyword.fetch!(opts, :reliability)
          |> reliability_to_qos()

        DataTransmitter.push_datastream(
          realm,
          device_id,
          interface,
          path,
          value,
          qos: qos
        )
    end
  end

  # Exactly one match, always good
  defp ensure_publish_reliability(local_matches, remote_matches, _opts)
       when local_matches + remote_matches == 1 do
    :ok
  end

  # Multiple matches, we print a warning but we consider it ok
  defp ensure_publish_reliability(local_matches, remote_matches, _opts)
       when local_matches + remote_matches > 1 do
    Logger.warning(
      "Multiple matches while publishing to device, " <>
        "local_matches: #{local_matches}, remote_matches: #{remote_matches}",
      tag: "publish_multiple_matches"
    )

    :ok
  end

  # No matches, check type and reliability
  defp ensure_publish_reliability(_local, _remote, opts) do
    type = Keyword.fetch!(opts, :type)
    # We use get since we can be in a properties case
    reliability = Keyword.get(opts, :reliability)

    cond do
      type == :properties ->
        # No matches will happen only if the device doesn't have a session on
        # the broker, but the SDK would then send an emptyCache at the first
        # connection and receive all properties. Hence, we return :ok for
        # properties even if there are no matches
        :ok

      type == :datastream and reliability == :unreliable ->
        # Unreliable datastream is allowed to fail
        :ok

      true ->
        {:error, :cannot_push_to_device}
    end
  end

  defp reliability_to_qos(reliability) do
    case reliability do
      :unreliable -> 0
      :guaranteed -> 1
      :unique -> 2
    end
  end

  defp validate_value_type(expected_types, object)
       when is_map(expected_types) and is_map(object) do
    Enum.reduce_while(object, :ok, fn {key, value}, _acc ->
      with {:ok, expected_type} <- Map.fetch(expected_types, key),
           :ok <- validate_value_type(expected_type, value) do
        {:cont, :ok}
      else
        {:error, reason, expected} ->
          {:halt, {:error, reason, expected}}

        :error ->
          {:halt, {:error, :unexpected_object_key}}
      end
    end)
  end

  defp validate_value_type(value_type, value) do
    with :ok <- ValueType.validate_value(value_type, value) do
      :ok
    else
      {:error, :unexpected_value_type} ->
        {:error, :unexpected_value_type, expected: value_type}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp wrap_to_bson_struct(:binaryblob, value) do
    # 0 is generic binary subtype
    {0, value}
  end

  defp wrap_to_bson_struct(:binaryblobarray, values) do
    Enum.map(values, &wrap_to_bson_struct(:binaryblob, &1))
  end

  defp wrap_to_bson_struct(expected_types, values)
       when is_map(expected_types) and is_map(values) do
    Enum.map(values, fn {key, value} ->
      # We can be sure this exists since we validated it in validate_value_type
      type = Map.fetch!(expected_types, key)
      {key, wrap_to_bson_struct(type, value)}
    end)
    |> Enum.into(%{})
  end

  defp wrap_to_bson_struct(_anytype, value) do
    value
  end

  # TODO: we should probably allow delete for every path regardless of the interface type
  # just for maintenance reasons
  def delete_interface_values(realm_name, encoded_device_id, interface, no_prefix_path) do
    with {:ok, device_id} <- Device.decode_device_id(encoded_device_id),
         {:ok, major_version} <-
           DeviceQueries.interface_version(realm_name, device_id, interface),
         {:ok, interface_row} <-
           InterfaceQueries.retrieve_interface_row(realm_name, interface, major_version),
         {:ok, interface_descriptor} <- InterfaceDescriptor.from_db_result(interface_row),
         {:ownership, :server} <- {:ownership, interface_descriptor.ownership},
         path <- "/" <> no_prefix_path,
         {:ok, [endpoint_id]} <- get_endpoint_ids(interface_descriptor.automaton, path) do
      mapping =
        Queries.retrieve_mapping(realm_name)
        |> Repo.get_by!(%{
          interface_id: interface_descriptor.interface_id,
          endpoint_id: endpoint_id
        })

      insert_value_into_db(
        realm_name,
        device_id,
        interface_descriptor,
        endpoint_id,
        mapping,
        path,
        nil,
        nil,
        []
      )

      case interface_descriptor.type do
        :properties ->
          unset_property(realm_name, device_id, interface, path)

        :datastream ->
          :ok
      end
    else
      {:ownership, :device} ->
        {:error, :cannot_write_to_device_owned}

      {:error, :endpoint_guess_not_allowed} ->
        {:error, :read_only_resource}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp unset_property(realm_name, device_id, interface, path) do
    # Do not check for matches, as the device receives the unset information anyway
    # (either when it reconnects or in the /control/consumerProperties message).
    # See https://github.com/astarte-platform/astarte/issues/640
    with {:ok, _} <- DataTransmitter.unset_property(realm_name, device_id, interface, path) do
      :ok
    end
  end

  defp do_get_interface_values!(realm_name, _client, device_id, :individual, interface_row, opts) do
    endpoint_rows =
      Queries.retrieve_all_endpoint_ids_for_interface!(realm_name, interface_row.interface_id)
      |> Repo.all()

    values_map =
      Enum.reduce(endpoint_rows, %{}, fn endpoint_row, values ->
        # TODO: we can do this by using just one query without any filter on the endpoint
        value =
          retrieve_endpoint_values(
            realm_name,
            device_id,
            interface_row.aggregation,
            interface_row.type,
            interface_row,
            endpoint_row.endpoint_id,
            endpoint_row,
            "/",
            opts
          )

        Map.merge(values, value)
      end)

    {:ok, %InterfaceValues{data: MapTree.inflate_tree(values_map)}}
  end

  defp do_get_interface_values!(realm_name, client, device_id, :object, interface_row, opts) do
    # We need to know if mappings have explicit_timestamp set, so we retrieve it from the
    # first one.
    endpoint =
      Queries.retrieve_all_endpoint_ids_for_interface!(realm_name, interface_row.interface_id)
      |> limit(1)
      |> Repo.one!()

    mapping =
      Queries.retrieve_mapping(realm_name)
      |> Repo.get_by!(%{
        interface_id: interface_row.interface_id,
        endpoint_id: endpoint.endpoint_id
      })

    do_get_interface_values!(
      realm_name,
      client,
      device_id,
      interface_row.aggregation,
      interface_row.type,
      interface_row,
      nil,
      "/",
      %{opts | explicit_timestamp: mapping.explicit_timestamp}
    )
  end

  defp do_get_interface_values!(
         realm_name,
         _client,
         device_id,
         :individual,
         :properties,
         interface_row,
         endpoint_ids,
         path,
         opts
       ) do
    result =
      List.foldl(endpoint_ids, %{}, fn endpoint_id, values ->
        endpoint_row =
          Queries.value_type_query(realm_name)
          |> Repo.get_by!(%{interface_id: interface_row.interface_id, endpoint_id: endpoint_id})

        value =
          retrieve_endpoint_values(
            realm_name,
            device_id,
            :individual,
            :properties,
            interface_row,
            endpoint_id,
            endpoint_row,
            path,
            opts
          )

        Map.merge(values, value)
      end)

    individual_value = Map.get(result, "")

    data =
      if individual_value != nil do
        individual_value
      else
        MapTree.inflate_tree(result)
      end

    {:ok, %InterfaceValues{data: data}}
  end

  defp do_get_interface_values!(
         realm_name,
         _client,
         device_id,
         :individual,
         :datastream,
         interface_row,
         endpoint_ids,
         path,
         opts
       ) do
    [endpoint_id] = endpoint_ids

    endpoint_row =
      Queries.value_type_query(realm_name)
      |> Repo.get_by!(%{interface_id: interface_row.interface_id, endpoint_id: endpoint_id})

    retrieve_endpoint_values(
      realm_name,
      device_id,
      :individual,
      :datastream,
      interface_row,
      endpoint_id,
      endpoint_row,
      path,
      opts
    )
  end

  defp do_get_interface_values!(
         realm_name,
         _client,
         device_id,
         :object,
         :datastream,
         interface_row,
         _endpoint_ids,
         path,
         opts
       ) do
    # We need to know if mappings have explicit_timestamp set, so we retrieve it from the
    # first one.
    endpoint =
      Queries.retrieve_all_endpoint_ids_for_interface!(realm_name, interface_row.interface_id)
      |> limit(1)
      |> Repo.one!()

    mapping =
      Queries.retrieve_mapping(realm_name)
      |> Repo.get_by!(%{
        interface_id: interface_row.interface_id,
        endpoint_id: endpoint.endpoint_id
      })

    endpoint_rows =
      Queries.retrieve_all_endpoints_for_interface!(realm_name, interface_row.interface_id)
      |> Repo.all()

    interface_values =
      retrieve_endpoint_values(
        realm_name,
        device_id,
        :object,
        :datastream,
        interface_row,
        nil,
        endpoint_rows,
        path,
        %{opts | explicit_timestamp: mapping.explicit_timestamp}
      )

    cond do
      path == "/" and interface_values == {:error, :path_not_found} ->
        {:ok, %InterfaceValues{data: %{}}}

      path != "/" and elem(interface_values, 1).data == [] ->
        {:error, :path_not_found}

      true ->
        interface_values
    end
  end

  # TODO: optimize: do not use string replace
  defp simplify_path(base_path, path) do
    no_basepath = String.replace_prefix(path, base_path, "")

    case no_basepath do
      "/" <> noleadingslash -> noleadingslash
      already_noleadingslash -> already_noleadingslash
    end
  end

  defp get_endpoint_ids(automaton, path, opts \\ []) do
    allow_guess = opts[:allow_guess]

    case EndpointsAutomaton.resolve_path(path, automaton) do
      {:ok, endpoint_id} ->
        {:ok, [endpoint_id]}

      {:guessed, endpoint_ids} when allow_guess ->
        {:ok, endpoint_ids}

      {:guessed, _endpoint_ids} ->
        {:error, :endpoint_guess_not_allowed}

      {:error, :not_found} ->
        {:error, :endpoint_not_found}
    end
  end

  defp retrieve_endpoint_values(
         realm_name,
         device_id,
         :individual,
         :datastream,
         interface_row,
         endpoint_id,
         endpoint_row,
         "/",
         opts
       ) do
    path = "/"
    interface_id = interface_row.interface_id

    value_column =
      CQLUtils.type_to_db_column_name(endpoint_row.value_type) |> String.to_atom()

    columns = default_endpoint_column_selection(value_column)

    values =
      Queries.retrieve_all_endpoint_paths!(realm_name, device_id, interface_id, endpoint_id)
      |> Repo.all()
      |> Enum.filter(fn endpoint -> endpoint[:path] |> String.starts_with?(path) end)
      |> Enum.reduce(%{}, fn row, values_map ->
        last_value =
          Queries.retrieve_datastream_values(
            realm_name,
            device_id,
            interface_row,
            endpoint_id,
            row.path,
            %{opts | limit: 1}
          )
          |> select(^columns)

        case Repo.fetch_one(last_value) do
          {:ok, value} ->
            %{^value_column => v, value_timestamp: tstamp, reception_timestamp: reception} = value
            simplified_path = simplify_path(path, row.path)

            nice_value =
              AstarteValue.to_json_friendly(
                v,
                endpoint_row.value_type,
                fetch_biginteger_opts_or_default(opts)
              )

            Map.put(values_map, simplified_path, %{
              "value" => nice_value,
              "timestamp" =>
                AstarteValue.to_json_friendly(
                  tstamp,
                  :datetime,
                  keep_milliseconds: opts.keep_milliseconds
                ),
              "reception_timestamp" =>
                AstarteValue.to_json_friendly(
                  reception,
                  :datetime,
                  keep_milliseconds: opts.keep_milliseconds
                )
            })

          {:error, _reason} ->
            %{}
        end
      end)

    values
  end

  defp retrieve_endpoint_values(
         realm_name,
         device_id,
         :object,
         :datastream,
         interface_row,
         nil,
         endpoint_row,
         "/",
         opts
       ) do
    path = "/"

    interface_id = interface_row.interface_id

    endpoint_id = CQLUtils.endpoint_id(interface_row.name, interface_row.major_version, "")

    {count, paths} =
      Queries.retrieve_all_endpoint_paths!(realm_name, device_id, interface_id, endpoint_id)
      |> Repo.all()
      |> Enum.reduce({0, []}, fn row, {count, all_paths} ->
        if String.starts_with?(row[:path], path) do
          {count + 1, [row.path | all_paths]}
        else
          {count, all_paths}
        end
      end)

    cond do
      count == 0 ->
        {:error, :path_not_found}

      count == 1 ->
        [only_path] = paths

        with {:ok,
              %Astarte.AppEngine.API.Device.InterfaceValues{data: values, metadata: metadata}} <-
               retrieve_endpoint_values(
                 realm_name,
                 device_id,
                 :object,
                 :datastream,
                 interface_row,
                 endpoint_id,
                 endpoint_row,
                 only_path,
                 opts
               ),
             {:ok, interface_values} <-
               get_interface_values_from_path(values, metadata, path, only_path) do
          {:ok, interface_values}
        else
          err ->
            Logger.warning("An error occurred while retrieving endpoint values: #{inspect(err)}",
              tag: "retrieve_endpoint_values_error"
            )

            err
        end

      count > 1 ->
        values_map =
          Enum.reduce(paths, %{}, fn a_path, values_map ->
            {:ok, %Astarte.AppEngine.API.Device.InterfaceValues{data: values}} =
              retrieve_endpoint_values(
                realm_name,
                device_id,
                :object,
                :datastream,
                interface_row,
                endpoint_id,
                endpoint_row,
                a_path,
                %{opts | limit: 1}
              )

            case values do
              [] ->
                values_map

              [value] ->
                simplified_path = simplify_path(path, a_path)

                Map.put(values_map, simplified_path, value)
            end
          end)
          |> MapTree.inflate_tree()

        {:ok, %InterfaceValues{data: values_map}}
    end
  end

  defp retrieve_endpoint_values(
         realm_name,
         device_id,
         :object,
         :datastream,
         interface_row,
         _endpoint_id,
         endpoint_rows,
         path,
         opts
       ) do
    # FIXME: reading result wastes atoms: new atoms are allocated every time a new table is seen
    # See cqerl_protocol.erl:330 (binary_to_atom), strings should be used when dealing with large schemas
    # https://github.com/elixir-ecto/ecto/pull/4384
    endpoints =
      endpoint_rows
      |> Enum.map(
        &%{
          column: &1.endpoint |> CQLUtils.endpoint_to_db_column_name() |> String.to_atom(),
          pretty_name: &1.endpoint |> String.split("/") |> List.last(),
          value_type: &1.value_type
        }
      )

    metadata = fn endpoint -> Map.take(endpoint, [:pretty_name, :value_type]) end
    columns = endpoints |> Enum.map(& &1.column)
    endpoint_metadata = endpoints |> Map.new(&{&1.column, metadata.(&1)})

    # The old implementation used the latest element it found for the downsample column.
    # Could we just drop the reverse and consider the first instead?
    downsample_column =
      endpoints
      |> Enum.reverse()
      |> Enum.find_value(&(&1.pretty_name == opts.downsample_key && &1.column))

    timestamp_column = timestamp_column(opts.explicit_timestamp)
    columns = [timestamp_column | columns]

    # {:ok, count, values} =
    query =
      Queries.retrieve_object_datastream_values(
        realm_name,
        device_id,
        interface_row,
        path,
        timestamp_column,
        opts
      )

    values = query |> select(^columns) |> Repo.all()
    count = query |> select([d], count(field(d, ^timestamp_column))) |> Repo.one!()

    values
    |> maybe_downsample_to(count, :object, nil, %InterfaceValuesOptions{
      opts
      | downsample_key: downsample_column
    })
    |> pack_result(:object, :datastream, endpoint_metadata, opts)
  end

  defp retrieve_endpoint_values(
         realm_name,
         device_id,
         :individual,
         :datastream,
         interface_row,
         endpoint_id,
         endpoint_row,
         path,
         opts
       ) do
    query =
      Queries.retrieve_datastream_values(
        realm_name,
        device_id,
        interface_row,
        endpoint_id,
        path,
        opts
      )

    value_column =
      CQLUtils.type_to_db_column_name(endpoint_row.value_type) |> String.to_atom()

    columns = default_endpoint_column_selection(value_column)

    values = query |> select(^columns) |> Repo.all()
    count = query |> select([d], count(d.value_timestamp)) |> Repo.one!()

    values
    |> maybe_downsample_to(count, :individual, value_column, opts)
    |> pack_result(:individual, :datastream, endpoint_row, path, opts)
  end

  defp retrieve_endpoint_values(
         realm_name,
         device_id,
         :individual,
         :properties,
         interface_row,
         endpoint_id,
         endpoint_row,
         path,
         opts
       ) do
    table_name = interface_row.storage
    interface_id = interface_row.interface_id

    value_column =
      CQLUtils.type_to_db_column_name(endpoint_row.value_type) |> String.to_atom()

    values =
      Queries.find_endpoints(
        realm_name,
        table_name,
        device_id,
        interface_id,
        endpoint_id
      )
      |> select(^[:path, value_column])
      |> Repo.all()
      |> Enum.filter(&String.starts_with?(&1.path, path))
      |> Enum.reduce(%{}, fn row, values_map ->
        %{^value_column => value, path: row_path} = row

        simplified_path = simplify_path(path, row_path)

        nice_value =
          AstarteValue.to_json_friendly(
            value,
            endpoint_row.value_type,
            fetch_biginteger_opts_or_default(opts)
          )

        Map.put(values_map, simplified_path, nice_value)
      end)

    values
  end

  defp get_interface_values_from_path([], _metadata, _path, _only_path) do
    {:ok, %InterfaceValues{data: %{}}}
  end

  defp get_interface_values_from_path(values, metadata, path, only_path) when is_list(values) do
    simplified_path = simplify_path(path, only_path)

    case simplified_path do
      "" ->
        {:ok, %InterfaceValues{data: values, metadata: metadata}}

      _ ->
        values_map =
          %{simplified_path => values}
          |> MapTree.inflate_tree()

        {:ok, %InterfaceValues{data: values_map, metadata: metadata}}
    end
  end

  defp get_interface_values_from_path(values, metadata, _path, _only_path) do
    {:ok, %InterfaceValues{data: values, metadata: metadata}}
  end

  defp maybe_downsample_to(values, _count, _aggregation, _value_column, %InterfaceValuesOptions{
         downsample_to: nil
       }) do
    values
  end

  defp maybe_downsample_to(values, nil, _aggregation, _value_column, _opts) do
    # TODO: we can't downsample an object without a valid count, propagate an error changeset
    # when we start using changeset consistently here
    _ = Logger.warning("No valid count in maybe_downsample_to.", tag: "downsample_invalid_count")
    values
  end

  defp maybe_downsample_to(values, _count, :object, _value_column, %InterfaceValuesOptions{
         downsample_key: nil
       }) do
    # TODO: we can't downsample an object without downsample_key, propagate an error changeset
    # when we start using changeset consistently here
    _ =
      Logger.warning("No valid downsample_key found in maybe_downsample_to.",
        tag: "downsample_invalid_key"
      )

    values
  end

  defp maybe_downsample_to(values, count, :object, _value_column, %InterfaceValuesOptions{
         downsample_to: downsampled_size,
         downsample_key: downsample_key,
         explicit_timestamp: explicit_timestamp
       })
       when downsampled_size > 2 do
    timestamp_column = timestamp_column(explicit_timestamp)
    avg_bucket_size = max(1, (count - 2) / (downsampled_size - 2))

    sample_to_x_fun = fn sample ->
      sample |> Map.fetch!(timestamp_column) |> DateTime.to_unix(:millisecond)
    end

    sample_to_y_fun = fn sample -> Map.fetch!(sample, downsample_key) end
    xy_to_sample_fun = fn x, y -> [{timestamp_column, x}, {downsample_key, y}] end

    ExLTTB.Stream.downsample(
      values,
      avg_bucket_size,
      sample_to_x_fun: sample_to_x_fun,
      sample_to_y_fun: sample_to_y_fun,
      xy_to_sample_fun: xy_to_sample_fun
    )
  end

  defp maybe_downsample_to(values, count, :individual, value_column, %InterfaceValuesOptions{
         downsample_to: downsampled_size
       })
       when downsampled_size > 2 do
    avg_bucket_size = max(1, (count - 2) / (downsampled_size - 2))

    sample_to_x_fun = fn sample -> sample.value_timestamp |> DateTime.to_unix(:millisecond) end
    sample_to_y_fun = fn sample -> Map.fetch!(sample, value_column) end

    xy_to_sample_fun = fn x, y -> [{:value_timestamp, x}, {:generic_key, y}] end

    ExLTTB.Stream.downsample(
      values,
      avg_bucket_size,
      sample_to_x_fun: sample_to_x_fun,
      sample_to_y_fun: sample_to_y_fun,
      xy_to_sample_fun: xy_to_sample_fun
    )
  end

  defp pack_result([] = _values, :individual, :datastream, _endpoint_row, _path, _opts),
    do: {:error, :path_not_found}

  defp pack_result(
         values,
         :individual,
         :datastream,
         endpoint_row,
         _path,
         %{format: "structured"} = opts
       ) do
    value_key = CQLUtils.type_to_db_column_name(endpoint_row.value_type) |> String.to_atom()

    values_array =
      for value <- values do
        %{^value_key => v, value_timestamp: tstamp} = value

        %{
          "timestamp" =>
            AstarteValue.to_json_friendly(
              tstamp,
              :datetime,
              keep_milliseconds: opts.keep_milliseconds
            ),
          "value" => AstarteValue.to_json_friendly(v, endpoint_row.value_type, [])
        }
      end

    {:ok,
     %InterfaceValues{
       data: values_array
     }}
  end

  defp pack_result(
         values,
         :individual,
         :datastream,
         endpoint_row,
         path,
         %{format: "table"} = opts
       ) do
    value_name =
      path
      |> String.split("/")
      |> List.last()

    value_key = CQLUtils.type_to_db_column_name(endpoint_row.value_type) |> String.to_atom()

    values_array =
      for value <- values do
        %{^value_key => v, value_timestamp: tstamp} = value

        [
          AstarteValue.to_json_friendly(tstamp, :datetime, []),
          AstarteValue.to_json_friendly(
            v,
            endpoint_row.value_type,
            keep_milliseconds: opts.keep_milliseconds
          )
        ]
      end

    {:ok,
     %InterfaceValues{
       metadata: %{
         "columns" => %{"timestamp" => 0, value_name => 1},
         "table_header" => ["timestamp", value_name]
       },
       data: values_array
     }}
  end

  defp pack_result(
         values,
         :individual,
         :datastream,
         endpoint_row,
         _path,
         %{format: "disjoint_tables"} = opts
       ) do
    value_key = CQLUtils.type_to_db_column_name(endpoint_row.value_type) |> String.to_atom()

    values_array =
      for value <- values do
        %{^value_key => v, value_timestamp: tstamp} = value

        [
          AstarteValue.to_json_friendly(v, endpoint_row.value_type, []),
          AstarteValue.to_json_friendly(
            tstamp,
            :datetime,
            keep_milliseconds: opts.keep_milliseconds
          )
        ]
      end

    {:ok,
     %InterfaceValues{
       data: %{"value" => values_array}
     }}
  end

  defp pack_result(
         values,
         :object,
         :datastream,
         column_metadata,
         %{format: "table"} = opts
       ) do
    data = object_datastream_pack(values, column_metadata, opts)

    table_header =
      case data do
        [] -> []
        [first | _] -> first |> Map.keys()
      end

    table_header_count = table_header |> Enum.count()
    columns = table_header |> Enum.zip(0..table_header_count) |> Map.new()

    values_array = data |> Enum.map(&Map.values/1)

    {:ok,
     %InterfaceValues{
       metadata: %{"columns" => columns, "table_header" => table_header},
       data: values_array
     }}
  end

  defp pack_result(
         values,
         :object,
         :datastream,
         column_metadata,
         %{format: "disjoint_tables"} = opts
       ) do
    data = object_datastream_multilist(values, column_metadata, opts)
    {timestamps, data} = data |> Map.pop!("timestamp")

    columns =
      for {column, values} <- data, into: %{} do
        values_with_timestamp =
          Enum.zip_with(values, timestamps, fn value, timestamp -> [value, timestamp] end)

        {column, values_with_timestamp}
      end

    {:ok, %InterfaceValues{data: columns}}
  end

  defp pack_result(
         values,
         :object,
         :datastream,
         column_metadata,
         %{format: "structured"} = opts
       ) do
    data = object_datastream_pack(values, column_metadata, opts)
    {:ok, %InterfaceValues{data: data}}
  end

  defp object_datastream_multilist([] = _values, _, _), do: []

  defp object_datastream_multilist(values, column_metadata, opts) do
    timestamp_column = timestamp_column(opts.explicit_timestamp)
    keep_milliseconds? = opts.keep_milliseconds

    headers = values |> hd() |> Map.keys()
    headers_without_timestamp = headers |> List.delete(timestamp_column)

    timestamp_data =
      for value <- values do
        value
        |> Map.get(timestamp_column)
        |> AstarteValue.to_json_friendly(:datetime, keep_milliseconds: keep_milliseconds?)
      end

    for header <- headers_without_timestamp, into: %{"timestamp" => timestamp_data} do
      %{pretty_name: name, value_type: type} = column_metadata |> Map.fetch!(header)

      values =
        for value <- values do
          value
          |> Map.fetch!(header)
          |> AstarteValue.to_json_friendly(type, [])
        end

      {name, values}
    end
  end

  defp object_datastream_pack(values, column_metadata, opts) do
    timestamp_column = timestamp_column(opts.explicit_timestamp)
    keep_milliseconds? = opts.keep_milliseconds

    for value <- values do
      timestamp_value =
        value
        |> Map.get(timestamp_column)
        |> AstarteValue.to_json_friendly(:datetime, keep_milliseconds: keep_milliseconds?)

      value
      |> Map.delete(timestamp_column)
      |> Map.take(column_metadata |> Map.keys())
      |> Map.new(fn {column, value} ->
        %{pretty_name: name, value_type: type} = column_metadata |> Map.fetch!(column)
        value = AstarteValue.to_json_friendly(value, type, [])

        {name, value}
      end)
      |> Map.put("timestamp", timestamp_value)
    end
  end

  def device_alias_to_device_id(realm_name, device_alias) do
    result =
      Queries.device_alias_to_device_id(realm_name, device_alias)
      |> Repo.fetch_one(consistency: :quorum, error: :device_not_found)

    with {:ok, name} <- result do
      {:ok, name.object_uuid}
    end
  end

  defp fetch_biginteger_opts_or_default(opts) do
    allow_bigintegers = Map.get(opts, :allow_bigintegers)
    allow_safe_bigintegers = Map.get(opts, :allow_safe_bigintegers)

    cond do
      allow_bigintegers ->
        [allow_bigintegers: allow_bigintegers]

      allow_safe_bigintegers ->
        [allow_safe_bigintegers: allow_safe_bigintegers]

      # Default allow_bigintegers to true in order to not break the existing API
      true ->
        [allow_bigintegers: true]
    end
  end

  defp timestamp_column(explicit_timestamp?) do
    case explicit_timestamp? do
      nil -> :reception_timestamp
      false -> :reception_timestamp
      true -> :value_timestamp
    end
  end

  defp default_endpoint_column_selection do
    [
      :value_timestamp,
      :reception_timestamp,
      :reception_timestamp_submillis
    ]
  end

  defp default_endpoint_column_selection(value_column) do
    [value_column | default_endpoint_column_selection()]
  end

  defp retrieve_device_status(realm_name, device_id) do
    device_query = Queries.device_status(realm_name)

    with {:ok, device} <- Repo.fetch(device_query, device_id, error: :device_not_found) do
      {:ok, DeviceStatus.from_device(device, realm_name)}
    end
  end

  def insert_path_into_db(
        realm_name,
        device_id,
        %InterfaceDescriptor{storage_type: storage_type} = interface_descriptor,
        endpoint_id,
        path,
        reception_timestamp,
        opts
      )
      when storage_type in [
             :multi_interface_individual_datastream_dbtable,
             :one_object_datastream_dbtable
           ] do
    keyspace = Realm.keyspace_name(realm_name)

    ttl = Keyword.get(opts, :ttl)
    opts = [prefix: keyspace, ttl: ttl]

    {reception_timestamp, timestamp_sub} = Queries.timestamp_and_submillis(reception_timestamp)

    # TODO: use received value_timestamp when needed
    # TODO: :reception_timestamp_submillis is just a place holder right now
    %DatabaseIndividualProperty{
      device_id: device_id,
      interface_id: interface_descriptor.interface_id,
      endpoint_id: endpoint_id,
      path: path,
      reception_timestamp: reception_timestamp,
      reception_timestamp_submillis: timestamp_sub,
      datetime_value: reception_timestamp
    }
    |> Repo.insert!(opts)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  def insert_value_into_db(
        realm_name,
        device_id,
        %InterfaceDescriptor{storage_type: :multi_interface_individual_properties_dbtable} =
          interface_descriptor,
        _endpoint_id,
        endpoint,
        path,
        nil,
        _timestamp,
        _opts
      ) do
    if endpoint.allow_unset == false do
      _ =
        Logger.warning("Tried to unset value on allow_unset=false mapping.",
          tag: "unset_not_allowed"
        )

      # TODO: should we handle this situation?
    end

    mapping =
      Queries.endpoint_mappings(realm_name, device_id, interface_descriptor, endpoint)
      |> where(path: ^path)

    Repo.delete_all(mapping)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  def insert_value_into_db(
        realm_name,
        device_id,
        %InterfaceDescriptor{storage_type: storage_type} = interface_descriptor,
        _endpoint_id,
        endpoint,
        path,
        value,
        timestamp,
        opts
      )
      when storage_type in [
             :multi_interface_individual_properties_dbtable,
             :multi_interface_individual_datastream_dbtable
           ] do
    keyspace = Realm.keyspace_name(realm_name)
    ttl = Keyword.get(opts, :ttl)
    # TODO: consistency = insert_consistency(interface_descriptor, endpoint)
    opts = [prefix: keyspace, ttl: ttl]

    args =
      %{
        device_id: device_id,
        interface_descriptor: interface_descriptor,
        endpoint: endpoint,
        path: path,
        timestamp: timestamp,
        value: value
      }

    entry = Queries.storage_attributes(storage_type, args)

    Repo.insert(entry, opts)
    :ok
  end

  def insert_value_into_db(
        realm_name,
        device_id,
        %InterfaceDescriptor{storage_type: storage_type} = interface_descriptor,
        _endpoint_id,
        _mapping,
        path,
        value,
        timestamp,
        opts
      )
      when storage_type == :one_object_datastream_dbtable do
    keyspace = Realm.keyspace_name(realm_name)

    interface_id = interface_descriptor.interface_id

    endpoints =
      from(DatabaseEndpoint, prefix: ^keyspace)
      |> select([:endpoint, :value_type])
      |> where(interface_id: ^interface_id)
      |> Repo.all()

    explicit_timestamp? =
      from(DatabaseEndpoint, prefix: ^keyspace)
      |> select([e], e.explicit_timestamp)
      |> where(interface_id: ^interface_id)
      |> limit(1)
      |> Repo.one()

    args = %{
      device_id: device_id,
      path: path,
      timestamp: timestamp,
      value: value,
      endpoints: endpoints,
      explicit_timestamp?: explicit_timestamp?
    }

    object_datastream = Queries.storage_attributes(storage_type, args)

    ttl = Keyword.get(opts, :ttl)
    # TODO: consistency = insert_consistency(interface_descriptor, endpoint)
    opts = [prefix: keyspace, ttl: ttl, returning: false]

    Repo.insert_all(interface_descriptor.storage, [object_datastream], opts)

    :ok
  end
end
