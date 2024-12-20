#
# This file is part of Astarte.
#
# Copyright 2017-2018 Ispirata Srl
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
#

defmodule Astarte.AppEngine.API.Device.Queries do
  import Ecto.Query

  alias Astarte.AppEngine.API.Config
  alias Astarte.AppEngine.API.Device.DeviceStatus
  alias Astarte.AppEngine.API.Device.InterfaceValuesOptions
  alias Astarte.AppEngine.API.Device.InterfaceInfo
  alias Astarte.Core.CQLUtils
  alias Astarte.Core.Device
  alias Astarte.Core.InterfaceDescriptor
  alias CQEx.Query, as: DatabaseQuery
  alias CQEx.Result, as: DatabaseResult

  alias Astarte.DataAccess.Realms.Device, as: DatabaseDevice
  alias Astarte.DataAccess.Realms.Endpoint, as: DatabaseEndpoint
  alias Astarte.DataAccess.Realms.DeletionInProgress, as: DatabaseDeletionInProgress
  alias Astarte.DataAccess.Realms.IndividualProperty, as: DatabaseIndividualProperty
  alias Astarte.DataAccess.Realms.IndividualDatastream, as: DatabaseIndividualDatastream
  alias Astarte.DataAccess.Astarte.KvStore
  alias Astarte.DataAccess.Astarte.Realm

  require CQEx
  require IEx
  require Logger

  def retrieve_interfaces_list(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)

    from DatabaseDevice,
      prefix: ^keyspace,
      select: [:introspection]
  end

  def retrieve_all_endpoint_ids_for_interface!(realm_name, interface_id) do
    keyspace = Realm.keyspace_name(realm_name)

    from DatabaseEndpoint,
      prefix: ^keyspace,
      where: [interface_id: ^interface_id],
      select: [:value_type, :endpoint_id]
  end

  def retrieve_all_endpoints_for_interface!(realm_name, interface_id) do
    keyspace = Realm.keyspace_name(realm_name)

    from DatabaseEndpoint,
      prefix: ^keyspace,
      where: [interface_id: ^interface_id],
      select: [:value_type, :endpoint]
  end

  def retrieve_mapping(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)

    from DatabaseEndpoint,
      prefix: ^keyspace,
      select: [
        :endpoint,
        :value_type,
        :reliability,
        :retention,
        :database_retention_policy,
        :database_retention_ttl,
        :expiry,
        :allow_unset,
        :endpoint_id,
        :interface_id,
        :explicit_timestamp
      ]
  end

  def datastream_maximum_storage_retention(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)

    from k in KvStore,
      prefix: ^keyspace,
      select: fragment("blobAsInt(?)", k.value),
      where: k.group == "realm_config" and k.key == "datastream_maximum_storage_retention"
  end

  def retrieve_all_endpoint_paths!(realm_name, device_id, interface_id, endpoint_id) do
    find_endpoints(realm_name, "individual_properties", device_id, interface_id, endpoint_id)
    |> select([:path])
  end

  defp get_ttl_string(opts) do
    with {:ok, value} when is_integer(value) <- Keyword.fetch(opts, :ttl) do
      "USING TTL #{to_string(value)}"
    else
      _any_error ->
        ""
    end
  end

  def insert_path_into_db(
        db_client,
        device_id,
        %InterfaceDescriptor{storage_type: storage_type} = interface_descriptor,
        endpoint_id,
        path,
        value_timestamp,
        reception_timestamp,
        opts
      )
      when storage_type in [
             :multi_interface_individual_datastream_dbtable,
             :one_object_datastream_dbtable
           ] do
    # TODO: use received value_timestamp when needed
    # TODO: :reception_timestamp_submillis is just a place holder right now

    datastream = %DatabaseIndividualDatastream{
      device_id: device_id,
      interface_id: interface_descriptor.interface_id,
      endpoint_id: endpoint_id,
      path: path,
      reception_timestamp: reception_timestamp |> div(1000),
      reception_timestamp_submillis: reception_timestamp |> rem(100),
      datetime_value: value_timestamp
    }
    # wip here
    query = Exandra.Connection.insert(keyspace, DatabaseIndividualDatastream.__)

    # datastream |> R
    IEx.pry()

    # insert_statement = """
    # INSERT INTO individual_properties
    #     (device_id, interface_id, endpoint_id, path,
    #     reception_timestamp, reception_timestamp_submillis, datetime_value)
    # VALUES (:device_id, :interface_id, :endpoint_id, :path, :reception_timestamp,
    #     :reception_timestamp_submillis, :datetime_value) #{ttl_string};
    # """

    # insert_query =
    #   DatabaseQuery.new()
    #   |> DatabaseQuery.statement(insert_statement)
    #   |> DatabaseQuery.put(:device_id, device_id)
    #   |> DatabaseQuery.put(:interface_id, interface_descriptor.interface_id)
    #   |> DatabaseQuery.put(:endpoint_id, endpoint_id)
    #   |> DatabaseQuery.put(:path, path)
    #   |> DatabaseQuery.put(:reception_timestamp, div(reception_timestamp, 1000))
    #   |> DatabaseQuery.put(:reception_timestamp_submillis, rem(reception_timestamp, 100))
    #   |> DatabaseQuery.put(:datetime_value, value_timestamp)

    # DatabaseQuery.call!(db_client, insert_query)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  def insert_value_into_db(
        db_client,
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

    # TODO: :reception_timestamp_submillis is just a place holder right now
    unset_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(
        "DELETE FROM #{interface_descriptor.storage} WHERE device_id=:device_id AND interface_id=:interface_id AND endpoint_id=:endpoint_id AND path=:path"
      )
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.put(:interface_id, interface_descriptor.interface_id)
      |> DatabaseQuery.put(:endpoint_id, endpoint.endpoint_id)
      |> DatabaseQuery.put(:path, path)

    DatabaseQuery.call!(db_client, unset_query)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  def insert_value_into_db(
        db_client,
        device_id,
        %InterfaceDescriptor{storage_type: :multi_interface_individual_properties_dbtable} =
          interface_descriptor,
        endpoint_id,
        endpoint,
        path,
        value,
        timestamp,
        opts
      ) do
    ttl_string = get_ttl_string(opts)

    # TODO: :reception_timestamp_submillis is just a place holder right now
    insert_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement("""
      INSERT INTO #{interface_descriptor.storage}
        (device_id, interface_id, endpoint_id, path, reception_timestamp,
          #{CQLUtils.type_to_db_column_name(endpoint.value_type)})
        VALUES (:device_id, :interface_id, :endpoint_id, :path, :reception_timestamp,
          :value) #{ttl_string};
      """)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.put(:interface_id, interface_descriptor.interface_id)
      |> DatabaseQuery.put(:endpoint_id, endpoint_id)
      |> DatabaseQuery.put(:path, path)
      |> DatabaseQuery.put(:reception_timestamp, div(timestamp, 1000))
      |> DatabaseQuery.put(:reception_timestamp_submillis, div(timestamp, 100))
      |> DatabaseQuery.put(:value, to_db_friendly_type(value))

    DatabaseQuery.call!(db_client, insert_query)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  def insert_value_into_db(
        db_client,
        device_id,
        %InterfaceDescriptor{storage_type: :multi_interface_individual_datastream_dbtable} =
          interface_descriptor,
        _endpoint_id,
        endpoint,
        path,
        value,
        timestamp,
        opts
      ) do
    ttl_string = get_ttl_string(opts)

    insert_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement("""
      INSERT INTO #{interface_descriptor.storage}
        (device_id, interface_id, endpoint_id, path, value_timestamp, reception_timestamp, reception_timestamp_submillis,
          #{CQLUtils.type_to_db_column_name(endpoint.value_type)})
        VALUES (:device_id, :interface_id, :endpoint_id, :path, :value_timestamp, :reception_timestamp,
          :reception_timestamp_submillis, :value) #{ttl_string};
      """)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.put(:interface_id, interface_descriptor.interface_id)
      |> DatabaseQuery.put(:endpoint_id, endpoint.endpoint_id)
      |> DatabaseQuery.put(:path, path)
      |> DatabaseQuery.put(:value_timestamp, div(timestamp, 1000))
      |> DatabaseQuery.put(:reception_timestamp, div(timestamp, 1000))
      |> DatabaseQuery.put(:reception_timestamp_submillis, rem(timestamp, 100))
      |> DatabaseQuery.put(:value, to_db_friendly_type(value))

    # TODO: |> DatabaseQuery.consistency(insert_consistency(interface_descriptor, endpoint))

    DatabaseQuery.call!(db_client, insert_query)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  def insert_value_into_db(
        db_client,
        device_id,
        %InterfaceDescriptor{storage_type: :one_object_datastream_dbtable} = interface_descriptor,
        _endpoint_id,
        _mapping,
        path,
        value,
        timestamp,
        opts
      ) do
    ttl_string = get_ttl_string(opts)

    endpoint_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(
        "SELECT endpoint, value_type FROM endpoints WHERE interface_id=:interface_id;"
      )
      |> DatabaseQuery.put(:interface_id, interface_descriptor.interface_id)

    endpoint_rows = DatabaseQuery.call!(db_client, endpoint_query)

    explicit_timestamp_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(
        "SELECT explicit_timestamp FROM endpoints WHERE interface_id=:interface_id LIMIT 1;"
      )
      |> DatabaseQuery.put(:interface_id, interface_descriptor.interface_id)

    [explicit_timestamp: explicit_timestamp] =
      DatabaseQuery.call!(db_client, explicit_timestamp_query)
      |> DatabaseResult.head()

    # FIXME: new atoms are created here, we should avoid this. We need to replace CQEx.
    column_atoms =
      Enum.reduce(endpoint_rows, %{}, fn endpoint, column_atoms_acc ->
        endpoint_name =
          endpoint[:endpoint]
          |> String.split("/")
          |> List.last()

        column_name = CQLUtils.endpoint_to_db_column_name(endpoint_name)

        Map.put(column_atoms_acc, endpoint_name, String.to_atom(column_name))
      end)

    {query_values, placeholders, query_columns} =
      Enum.reduce(value, {%{}, "", ""}, fn {obj_key, obj_value},
                                           {query_values_acc, placeholders_acc, query_acc} ->
        if column_atoms[obj_key] != nil do
          column_name = CQLUtils.endpoint_to_db_column_name(obj_key)

          db_value = to_db_friendly_type(obj_value)
          next_query_values_acc = Map.put(query_values_acc, column_atoms[obj_key], db_value)
          next_placeholders_acc = "#{placeholders_acc} :#{to_string(column_atoms[obj_key])},"
          next_query_acc = "#{query_acc} #{column_name}, "

          {next_query_values_acc, next_placeholders_acc, next_query_acc}
        else
          Logger.warning(
            "Unexpected object key #{inspect(obj_key)} with value #{inspect(obj_value)}."
          )

          query_values_acc
        end
      end)

    {query_columns, placeholders} =
      if explicit_timestamp do
        {"value_timestamp, #{query_columns}", ":value_timestamp, #{placeholders}"}
      else
        {query_columns, placeholders}
      end

    # TODO: :reception_timestamp_submillis is just a place holder right now
    insert_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement("""
      INSERT INTO #{interface_descriptor.storage} (device_id, path, #{query_columns} reception_timestamp, reception_timestamp_submillis)
        VALUES (:device_id, :path, #{placeholders} :reception_timestamp, :reception_timestamp_submillis) #{ttl_string};
      """)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.put(:path, path)
      |> DatabaseQuery.put(:value_timestamp, div(timestamp, 1000))
      |> DatabaseQuery.put(:reception_timestamp, div(timestamp, 1000))
      |> DatabaseQuery.put(:reception_timestamp_submillis, rem(timestamp, 100))
      |> DatabaseQuery.merge(query_values)

    # TODO: |> DatabaseQuery.consistency(insert_consistency(interface_descriptor, endpoint))

    DatabaseQuery.call!(db_client, insert_query)

    :ok
  end

  # TODO Copy&pasted from data updater plant, make it a library
  defp to_db_friendly_type(array) when is_list(array) do
    # If we have an array, we convert its elements to a db friendly type
    Enum.map(array, &to_db_friendly_type/1)
  end

  defp to_db_friendly_type(%DateTime{} = datetime) do
    DateTime.to_unix(datetime, :millisecond)
  end

  defp to_db_friendly_type(value) do
    value
  end

  @device_status_columns_without_device_id [
    :aliases,
    :introspection,
    :introspection_minor,
    :connected,
    :last_connection,
    :last_disconnection,
    :first_registration,
    :first_credentials_request,
    :last_credentials_request_ip,
    :last_seen_ip,
    :attributes,
    :total_received_msgs,
    :total_received_bytes,
    :exchanged_msgs_by_interface,
    :exchanged_bytes_by_interface,
    :groups,
    :old_introspection,
    :inhibit_credentials_request
  ]

  def device_status(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)
    field_selection = [:device_id | @device_status_columns_without_device_id]

    from DatabaseDevice, prefix: ^keyspace, select: ^field_selection
  end

  def deletion_in_progress(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)

    from d in DatabaseDeletionInProgress, prefix: ^keyspace, select: [:device_id]
  end

  def retrieve_devices_list(realm_name, limit, retrieve_details, previous_token) do
    keyspace = Realm.keyspace_name(realm_name)

    field_selection =
      if retrieve_details do
        [:device_id | @device_status_columns_without_device_id]
      else
        [:device_id]
      end

    token_filter =
      case previous_token do
        nil ->
          true

        first ->
          min_token = first + 1
          dynamic([d], fragment("TOKEN(?)", d.device_id) >= ^min_token)
      end

    from d in DatabaseDevice,
      prefix: ^keyspace,
      select: merge(map(d, ^field_selection), %{"token" => fragment("TOKEN(?)", d.device_id)}),
      where: ^token_filter,
      limit: ^limit
  end

  def device_alias_to_device_id(client, device_alias) do
    device_id_statement = """
    SELECT object_uuid
    FROM names
    WHERE object_name = :device_alias AND object_type = 1
    """

    device_id_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(device_id_statement)
      |> DatabaseQuery.put(:device_alias, device_alias)
      |> DatabaseQuery.consistency(:quorum)

    with {:ok, result} <- DatabaseQuery.call(client, device_id_query),
         [object_uuid: device_id] <- DatabaseResult.head(result) do
      {:ok, device_id}
    else
      :empty_dataset ->
        {:error, :device_not_found}

      not_ok ->
        _ = Logger.warning("Database error: #{inspect(not_ok)}.", tag: "db_error")
        {:error, :database_error}
    end
  end

  def insert_attribute(client, device_id, attribute_key, attribute_value) do
    insert_attribute_statement = """
    UPDATE devices
    SET attributes[:attribute_key] = :attribute_value
    WHERE device_id = :device_id
    """

    query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(insert_attribute_statement)
      |> DatabaseQuery.put(:attribute_key, attribute_key)
      |> DatabaseQuery.put(:attribute_value, attribute_value)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.consistency(:each_quorum)

    with {:ok, _result} <- DatabaseQuery.call(client, query) do
      :ok
    else
      %{acc: _, msg: error_message} ->
        _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
        {:error, :database_error}

      {:error, reason} ->
        _ = Logger.warning("Database error, reason: #{inspect(reason)}.", tag: "db_error")
        {:error, :database_error}
    end
  end

  def delete_attribute(client, device_id, attribute_key) do
    retrieve_attribute_statement = """
    SELECT attributes FROM devices WHERE device_id = :device_id
    """

    retrieve_attribute_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(retrieve_attribute_statement)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.consistency(:quorum)

    with {:ok, result} <- DatabaseQuery.call(client, retrieve_attribute_query),
         [attributes: attributes] <- DatabaseResult.head(result),
         {^attribute_key, _attribute_value} <-
           Enum.find(attributes || [], fn m -> match?({^attribute_key, _}, m) end) do
      delete_attribute_statement = """
        DELETE attributes[:attribute_key]
        FROM devices
        WHERE device_id = :device_id
      """

      delete_attribute_query =
        DatabaseQuery.new()
        |> DatabaseQuery.statement(delete_attribute_statement)
        |> DatabaseQuery.put(:attribute_key, attribute_key)
        |> DatabaseQuery.put(:device_id, device_id)
        |> DatabaseQuery.consistency(:each_quorum)

      case DatabaseQuery.call(client, delete_attribute_query) do
        {:ok, _result} ->
          :ok

        %{acc: _, msg: error_message} ->
          _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
          {:error, :database_error}

        {:error, reason} ->
          _ = Logger.warning("Database error, reason: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    else
      nil ->
        {:error, :attribute_key_not_found}

      %{acc: _, msg: error_message} ->
        _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
        {:error, :database_error}

      {:error, reason} ->
        _ = Logger.warning("Database error, reason: #{inspect(reason)}.", tag: "db_error")
        {:error, :database_error}
    end
  end

  def insert_alias(client, device_id, alias_tag, alias_value) do
    insert_alias_to_names_statement = """
    INSERT INTO names
    (object_name, object_type, object_uuid)
    VALUES (:alias, 1, :device_id)
    """

    insert_alias_to_names_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(insert_alias_to_names_statement)
      |> DatabaseQuery.put(:alias, alias_value)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.consistency(:each_quorum)
      |> DatabaseQuery.convert()

    insert_alias_to_device_statement = """
    UPDATE devices
    SET aliases[:alias_tag] = :alias
    WHERE device_id = :device_id
    """

    insert_alias_to_device_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(insert_alias_to_device_statement)
      |> DatabaseQuery.put(:alias_tag, alias_tag)
      |> DatabaseQuery.put(:alias, alias_value)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.consistency(:each_quorum)
      |> DatabaseQuery.convert()

    insert_batch =
      CQEx.cql_query_batch(
        consistency: :each_quorum,
        mode: :logged,
        queries: [insert_alias_to_names_query, insert_alias_to_device_query]
      )

    with {:existing, {:error, :device_not_found}} <-
           {:existing, device_alias_to_device_id(client, alias_value)},
         :ok <- try_delete_alias(client, device_id, alias_tag),
         {:ok, _result} <- DatabaseQuery.call(client, insert_batch) do
      :ok
    else
      {:existing, {:ok, _device_uuid}} ->
        {:error, :alias_already_in_use}

      {:existing, {:error, reason}} ->
        {:error, reason}

      {:error, :device_not_found} ->
        {:error, :device_not_found}

      %{acc: _, msg: error_message} ->
        _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
        {:error, :database_error}

      {:error, reason} ->
        _ = Logger.warning("Database error, reason: #{inspect(reason)}.", tag: "db_error")
        {:error, :database_error}
    end
  end

  def delete_alias(client, device_id, alias_tag) do
    retrieve_aliases_statement = """
    SELECT aliases
    FROM devices
    WHERE device_id = :device_id
    """

    retrieve_aliases_query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(retrieve_aliases_statement)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.consistency(:quorum)

    with {:ok, result} <- DatabaseQuery.call(client, retrieve_aliases_query),
         [aliases: aliases] <- DatabaseResult.head(result),
         {^alias_tag, alias_value} <-
           Enum.find(aliases || [], fn a -> match?({^alias_tag, _}, a) end),
         {:check, {:ok, ^device_id}} <- {:check, device_alias_to_device_id(client, alias_value)} do
      delete_alias_from_device_statement = """
      DELETE aliases[:alias_tag]
      FROM devices
      WHERE device_id = :device_id
      """

      delete_alias_from_device_query =
        DatabaseQuery.new()
        |> DatabaseQuery.statement(delete_alias_from_device_statement)
        |> DatabaseQuery.put(:alias_tag, alias_tag)
        |> DatabaseQuery.put(:device_id, device_id)
        |> DatabaseQuery.consistency(:each_quorum)
        |> DatabaseQuery.convert()

      delete_alias_from_names_statement = """
      DELETE FROM names
      WHERE object_name = :alias AND object_type = 1
      """

      delete_alias_from_names_query =
        DatabaseQuery.new()
        |> DatabaseQuery.statement(delete_alias_from_names_statement)
        |> DatabaseQuery.put(:alias, alias_value)
        |> DatabaseQuery.put(:device_id, device_id)
        |> DatabaseQuery.consistency(:each_quorum)
        |> DatabaseQuery.convert()

      delete_batch =
        CQEx.cql_query_batch(
          consistency: :each_quorum,
          mode: :logged,
          queries: [delete_alias_from_device_query, delete_alias_from_names_query]
        )

      with {:ok, _result} <- DatabaseQuery.call(client, delete_batch) do
        :ok
      else
        %{acc: _, msg: error_message} ->
          _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
          {:error, :database_error}

        {:error, reason} ->
          _ = Logger.warning("Database error, reason: #{inspect(reason)}.", tag: "db_error")
          {:error, :database_error}
      end
    else
      {:check, _} ->
        _ =
          Logger.error("Inconsistent alias for #{alias_tag}.",
            device_id: device_id,
            tag: "inconsistent_alias"
          )

        {:error, :database_error}

      :empty_dataset ->
        {:error, :device_not_found}

      nil ->
        {:error, :alias_tag_not_found}

      %{acc: _, msg: error_message} ->
        _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
        {:error, :database_error}

      {:error, reason} ->
        _ = Logger.warning("Database error, reason: #{inspect(reason)}.", tag: "db_error")
        {:error, :database_error}
    end
  end

  defp try_delete_alias(client, device_id, alias_tag) do
    case delete_alias(client, device_id, alias_tag) do
      :ok ->
        :ok

      {:error, :alias_tag_not_found} ->
        :ok

      not_ok ->
        not_ok
    end
  end

  def set_inhibit_credentials_request(client, device_id, inhibit_credentials_request) do
    statement = """
    UPDATE devices
    SET inhibit_credentials_request = :inhibit_credentials_request
    WHERE device_id = :device_id
    """

    query =
      DatabaseQuery.new()
      |> DatabaseQuery.statement(statement)
      |> DatabaseQuery.put(:inhibit_credentials_request, inhibit_credentials_request)
      |> DatabaseQuery.put(:device_id, device_id)
      |> DatabaseQuery.consistency(:each_quorum)

    with {:ok, _result} <- DatabaseQuery.call(client, query) do
      :ok
    else
      %{acc: _, msg: error_message} ->
        _ = Logger.warning("Database error: #{error_message}.", tag: "db_error")
        {:error, :database_error}

      {:error, reason} ->
        _ = Logger.warning("Update failed, reason: #{inspect(reason)}.", tag: "db_error")
        {:error, :database_error}
    end
  end

  def retrieve_object_datastream_values(
        realm_name,
        device_id,
        interface_row,
        path,
        timestamp_column,
        opts
      ) do
    keyspace = Realm.keyspace_name(realm_name)

    # Check the explicit user defined limit to know if we have to reorder data
    data_ordering = if explicit_limit?(opts), do: [desc: timestamp_column], else: []
    query_limit = query_limit(opts)

    from(interface_row.storage, prefix: ^keyspace)
    |> where(device_id: ^device_id, path: ^path)
    |> filter_timestamp_range(timestamp_column, opts)
    |> order_by(^data_ordering)
    |> limit(^query_limit)
  end

  def get_results_count(_client, _count_query, %InterfaceValuesOptions{downsample_to: nil}) do
    # Count will be ignored since theres no downsample_to
    nil
  end

  def get_results_count(client, count_query, opts) do
    with {:ok, result} <- DatabaseQuery.call(client, count_query),
         [{_count_key, count}] <- DatabaseResult.head(result) do
      limit = opts.limit || Config.max_results_limit!()

      min(count, limit)
    else
      error ->
        _ =
          Logger.warning("Can't retrieve count for #{inspect(count_query)}: #{inspect(error)}.",
            tag: "db_error"
          )

        nil
    end
  end

  def all_properties_for_endpoint!(realm_name, device_id, interface_row, endpoint_id) do
    table = interface_row.storage
    interface_id = interface_row.interface_id

    value_type_column = Astarte.Core.CQLUtils.type_to_db_column_name(interface_row.storage_type)

    find_endpoints(realm_name, table, device_id, interface_id, endpoint_id)
    |> select(^[:path, value_type_column])
  end

  def find_endpoints(realm_name, table_name, device_id, interface_id, endpoint_id) do
    keyspace = Realm.keyspace_name(realm_name)

    from(table_name, prefix: ^keyspace)
    |> where(device_id: ^device_id, interface_id: ^interface_id, endpoint_id: ^endpoint_id)
  end

  def retrieve_datastream_values(
        realm_name,
        device_id,
        interface_row,
        endpoint_id,
        path,
        opts
      ) do
    keyspace = Realm.keyspace_name(realm_name)

    query_limit = query_limit(opts)

    # Check the explicit user defined limit to know if we have to reorder data
    data_ordering =
      if explicit_limit?(opts),
        do: [
          desc: :value_timestamp,
          desc: :reception_timestamp,
          desc: :reception_timestamp_submillis
        ],
        else: []

    storage_id = [
      device_id: device_id,
      interface_id: interface_row.interface_id,
      endpoint_id: endpoint_id,
      path: path
    ]

    from(interface_row.storage, prefix: ^keyspace)
    |> where(^storage_id)
    |> filter_timestamp_range(:value_timestamp, opts)
    |> order_by(^data_ordering)
    |> limit(^query_limit)
  end

  def value_type_query(realm_name) do
    keyspace = Realm.keyspace_name(realm_name)

    from DatabaseEndpoint,
      prefix: ^keyspace,
      select: [:value_type]
  end

  defp query_limit(opts), do: min(opts.limit, Config.max_results_limit!())

  defp explicit_limit?(opts) do
    user_defined_limit? = opts.limit != nil
    no_lower_timestamp_limit? = is_nil(opts.since) and is_nil(opts.since_after)

    user_defined_limit? and no_lower_timestamp_limit?
  end

  defp filter_timestamp_range(query, timestamp_column, query_opts) do
    filter_since =
      case {query_opts.since, query_opts.since_after} do
        {nil, nil} -> true
        {nil, since_after} -> dynamic([o], field(o, ^timestamp_column) > ^since_after)
        {since, _} -> dynamic([o], field(o, ^timestamp_column) >= ^since)
      end

    filter_to =
      case query_opts.to do
        nil -> true
        to -> dynamic([o], field(o, ^timestamp_column) < ^to)
      end

    query
    |> where(^filter_since)
    |> where(^filter_to)
  end
end
