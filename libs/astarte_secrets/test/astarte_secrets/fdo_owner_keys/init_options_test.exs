defmodule Astarte.Secrets.FDOOwnerKeys.InitOptionsTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Astarte.Secrets.FDOOwnerKeys.InitOptions
  alias Astarte.Secrets.FDOOwnerKeys
  alias COSE.Keys
  alias COSE.Keys.ECC
  alias Ecto.Changeset

  setup_all do
    key_name = "owner_key"

    p256_key_pem =
      """
      -----BEGIN EC PRIVATE KEY-----
      MHcCAQEEIFlbTEE1Ce+RSqhU8FqxsY7eNb9BaBWOTw6qFv7l0DZtoAoGCCqGSM49
      AwEHoUQDQgAEocPEIHIrn08VRO5zkkDztwp72Sw0BSm0mZeLgOKkHLUPdVFFlc0E
      O82b1/S2Cwzwh8MIDDx0CN2b+IBl5bRwOw==
      -----END EC PRIVATE KEY-----
      """

    {:ok, ecc_key} = Keys.from_pem(p256_key_pem)
    assert is_struct(ecc_key, ECC)

    %{key_name: key_name, p256_key_pem: p256_key_pem, ecc_key: ecc_key}
  end

  describe "changeset/2 with upload" do
    setup :populate_params

    test "populates a valid pem key", context do
      %{p256_params: params, ecc_key: ecc_key} = context

      changeset =
        InitOptions.changeset(%InitOptions{}, params)

      assert {:ok, %InitOptions{key: ^ecc_key}} = Changeset.apply_action(changeset, :insert)
    end
  end

  defp populate_params(context) do
    %{key_name: key_name, p256_key_pem: key_pem} = context

    p256_params =
      %{
        action: "upload",
        key_name: key_name,
        key_data: key_pem
      }

    %{p256_params: p256_params}
  end
end
