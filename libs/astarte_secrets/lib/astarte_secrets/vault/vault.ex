defmodule Astarte.Secrets.Vault do
  @moduledoc """
  Functionality to interface with OpenBao APIs.
  """

  alias Astarte.Secrets.Vault.Key
  alias Astarte.Secrets.Vault.Client
  alias Astarte.Secrets.Vault.Core
  alias HTTPoison.Response

  require Logger

  @spec get_key(String.t()) :: {:ok, map()} | {:error, term()}
  def get_key(key_name, opts \\ []) do
    namespace = Keyword.fetch!(opts, :namespace)

    with {:ok, resp} <- Core.get_key(key_name, namespace),
         {:ok, data} <- Core.parse_json_data(resp) do
      Key.parse(key_name, namespace, data)
    end
  end

  @spec list_keys_names() :: {:ok, [String.t()]} | :error
  def list_keys_names(opts \\ []) do
    namespace = Keyword.fetch!(opts, :namespace)

    Core.list_keys(namespace)
  end

  def list_namespaces do
    with {:ok, namespaces} <- Core.list_namespaces() do
      {:ok, Enum.to_list(namespaces)}
    end
  end

  @spec create_keypair(String.t(), Core.key_algorithm(), list()) ::
          {:ok, Key.t()} | {:error, Jason.DecodeError.t()} | :error
  def create_keypair(key_name, key_type, options \\ []) do
    namespace = Keyword.fetch!(options, :namespace)
    allow_key_export_and_backup = Keyword.get(options, :allow_key_export_and_backup, false)

    with {:ok, key_type_string} <- Core.key_type_to_string(key_type),
         {:ok, key_data} <-
           Core.create_keypair(key_name, key_type_string, allow_key_export_and_backup, namespace) do
      Key.parse(key_name, namespace, key_data)
    end
  end

  @spec enable_key_deletion(String.t(), list()) :: :ok | :error
  def enable_key_deletion(key_name, options \\ []) do
    req_body = %{deletion_allowed: true} |> Jason.encode!()

    headers = [{"Content-Type", "application/json"}]

    case Client.post("/transit/keys/#{key_name}/config", req_body, headers, options) do
      {:ok, %Response{status_code: 200}} ->
        :ok

      error_resp ->
        Logger.error(
          "Encountered HTTP error while enabling key deletion for key #{key_name}: #{inspect(error_resp)}"
        )

        :error
    end
  end

  @spec delete_key(String.t(), list()) :: :ok | :error
  def delete_key(key_name, options \\ []) do
    headers = []

    case Client.delete("/transit/keys/#{key_name}", headers, options) do
      {:ok, %Response{status_code: 204}} ->
        :ok

      error_resp ->
        Logger.error(
          "Encountered HTTP error while deleting key #{key_name}: #{inspect(error_resp)}"
        )

        :error
    end
  end

  @spec import_key(String.t(), Core.key_algorithm(), COSE.Keys.Key.t(), list()) :: :ok | :error
  def import_key(key_name, key_type, key, opts \\ []) do
    namespace = Keyword.fetch!(opts, :namespace)
    client_opts = [namespace: namespace] ++ Keyword.take(opts, [:token])

    with {:ok, key_type_string} <- Core.key_type_to_string(key_type),
         {:ok, wrapping_key_pem} <- Core.get_wrapping_key(client_opts),
         {:ok, ciphertext} <-
           Core.prepare_import_ciphertext(Core.encode_key_to_pkcs8(key), wrapping_key_pem) do
      Core.import_key(key_name, key_type_string, ciphertext, opts)
    end
  end

  @doc """
  Decrypts the provided ciphertext using OpenBao Transit Engine.
  Useful for ASYMKEX where the device encrypts a secret with the owner's RSA public key.
  """
  @spec decrypt(String.t(), binary(), list()) :: {:ok, binary()} | :error
  def decrypt(key_name, ciphertext, options \\ []) do
    namespace = Keyword.fetch!(options, :namespace)
    client_opts = [namespace: namespace] ++ Keyword.take(options, [:token])

    req_body =
      %{
        ciphertext: "vault:v1:" <> Base.encode64(ciphertext)
      }
      |> Jason.encode!()

    headers = [{"Content-Type", "application/json"}]

    case Client.post("/transit/decrypt/#{key_name}", req_body, headers, client_opts) do
      {:ok, %Response{status_code: 200, body: body}} ->
        with {:ok, data} <- Core.parse_json_data(body),
             plaintext_b64 when is_binary(plaintext_b64) <- Map.get(data, "plaintext"),
             {:ok, plaintext} <- Base.decode64(plaintext_b64) do
          {:ok, plaintext}
        else
          _ -> :error
        end

      error_resp ->
        Logger.error(
          "Encountered HTTP error while decrypting with key #{key_name}: #{inspect(error_resp)}"
        )

        :error
    end
  end

  @doc """
  Rotate the given key
  """
  def rotate(key_name, namespace) do
    path = "/transit/keys/#{key_name}/rotate"
    opts = [namespace: namespace]

    with {:ok, %Response{status_code: 200, body: resp}} <- Client.post(path, "", [], opts),
         {:ok, data} <- Core.parse_json_data(resp),
         {:ok, key} <- Key.parse(key_name, namespace, data) do
      {:ok, key}
    else
      error ->
        "Error while rotating key #{key_name} in namespace #{namespace}: #{inspect(error)}"
        |> Logger.error()

        :error
    end
  end
end
