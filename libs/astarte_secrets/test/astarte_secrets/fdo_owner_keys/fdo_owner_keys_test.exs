defmodule Astarte.Secrets.FDOOwnerKeysTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Astarte.Core.Device
  alias Astarte.DataAccess.FDO.OwnershipVoucher
  alias Astarte.DataAccess.FDO.Queries
  alias Astarte.Secrets.FDOOwnerKeys.InitOptions
  alias Astarte.Secrets.FDOOwnerKeys
  alias Astarte.Secrets.Vault.Key

  import ExUnit.CaptureLog

  describe "fetch/4" do
    setup :create_es256_key

    test "returns {:ok, key} when the key exists", context do
      %{realm_name: realm_name, key_name: key_name, key_algorithm: key_algorithm} = context

      assert {:ok, key} = FDOOwnerKeys.fetch(realm_name, key_name, key_algorithm)
      assert %Key{name: ^key_name, alg: ^key_algorithm} = key
    end

    test "returns :not_found when key does not exist", context do
      %{realm_name: realm_name, key_algorithm: key_algorithm} = context
      not_existing_key = "not_existing_key"

      assert {:error, :not_found} =
               FDOOwnerKeys.fetch(realm_name, not_existing_key, key_algorithm)
    end

    test "returns :not_found when key exists under a different algorithm", context do
      %{realm_name: realm_name, key_name: key_name} = context
      wrong_key_algorithm = :es384

      assert {:error, :not_found} =
               FDOOwnerKeys.fetch(realm_name, key_name, wrong_key_algorithm)
    end
  end

  describe "fetch_device_owner_key/4" do
    setup :create_es256_key
    setup :add_device_key

    test "returns {:ok, key} when the device has an associated key and the key exists", context do
      %{realm_name: realm_name, key_name: key_name, key_algorithm: key_algorithm, guid: guid} =
        context

      assert {:ok, key} = FDOOwnerKeys.fetch_device_owner_key(realm_name, guid)
      assert %Key{name: ^key_name, alg: ^key_algorithm} = key
    end

    test "returns error when the entry does not exist", context do
      %{realm_name: realm_name, guid: guid} = context

      Queries
      |> expect(:get_owner_key_params, fn ^realm_name, ^guid -> {:error, :not_found} end)

      assert {:error, :not_found} = FDOOwnerKeys.fetch_device_owner_key(realm_name, guid)
    end

    test "returns error when the key does not exist", context do
      %{realm_name: realm_name, key_algorithm: key_algorithm, guid: guid} =
        context

      invalid_key_name = "invalid"

      Queries
      |> expect(:get_owner_key_params, fn ^realm_name, ^guid ->
        {:ok, %{name: invalid_key_name, algorithm: key_algorithm}}
      end)

      assert {:error, :not_found} = FDOOwnerKeys.fetch_device_owner_key(realm_name, guid)
    end
  end

  describe "create/5" do
  end

  describe "import_key/5" do
  end

  describe "delete/4" do
    setup :create_es256_key

    test "deletes a key", context do
      %{realm_name: realm_name, key_name: key_name, key_algorithm: key_algorithm} = context

      assert :ok = FDOOwnerKeys.delete(realm_name, key_name, key_algorithm)

      assert {:error, :not_found} =
               FDOOwnerKeys.fetch(realm_name, key_name, key_algorithm)
    end

    test "returns :error when the key does not exist", context do
      %{realm_name: realm_name, key_algorithm: key_algorithm} = context

      invalid_key_name = "invalid"

      {result, log} =
        with_log(fn -> FDOOwnerKeys.delete(realm_name, invalid_key_name, key_algorithm) end)

      assert :error = result
      assert log =~ "no existing key"
    end
  end

  defp create_es256_key(_context) do
    unique_id = System.unique_integer([:positive])
    realm_name = "findtest_#{unique_id}"
    key_name = "find-me"
    key_algorithm = :es256
    opts = [allow_key_export_and_backup: true]

    on_exit(fn ->
      capture_log(fn ->
        FDOOwnerKeys.delete(realm_name, key_name, key_algorithm)
      end)
    end)

    FDOOwnerKeys.create(realm_name, nil, key_name, key_algorithm, opts)

    %{realm_name: realm_name, key_name: key_name, key_algorithm: key_algorithm}
  end

  defp add_device_key(context) do
    %{realm_name: realm_name, key_name: key_name, key_algorithm: key_algorithm} = context
    guid = Device.random_device_id()

    # we don't setup the database currently
    Queries
    |> stub(:get_owner_key_params, fn ^realm_name, ^guid ->
      {:ok, %{name: key_name, algorithm: key_algorithm}}
    end)

    %{guid: guid}
  end
end
