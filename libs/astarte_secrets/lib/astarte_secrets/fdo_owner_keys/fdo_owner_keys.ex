defmodule Astarte.Secrets.FDOOwnerKeys do
  alias Astarte.DataAccess.FDO.Queries
  alias Astarte.Secrets.Vault
  alias Astarte.Secrets.FDOOwnerKeys.InitOptions

  def fetch(realm_name, user_id \\ nil, key_name, key_algorithm) do
    with {:ok, namespace} <- Vault.owner_key_namespace(realm_name, user_id, key_algorithm) do
      Vault.get_key(key_name, namespace: namespace)
    end
  end

  def fetch_device_owner_key(realm_name, user_id \\ nil, guid) do
    with {:ok, params} <- Queries.get_owner_key_params(realm_name, guid),
         {:ok, namespace} <-
           Vault.owner_key_namespace(realm_name, user_id, params.algorithm) do
      Vault.get_key(params.name, namespace: namespace)
    end
  end

  def init(
        realm_name,
        %InitOptions{action: :create} = init_opts
      ) do
    %InitOptions{
      key_name: key_name,
      key_algorithm: key_algorithm
    } = init_opts

    create(realm_name, key_name, key_algorithm)
  end

  def init(
        realm_name,
        %InitOptions{action: :upload} = init_opts
      ) do
    %InitOptions{
      key_name: key_name,
      key: key
    } = init_opts

    import_key(realm_name, key_name, key.alg, key)
  end

  def create(realm_name, user_id \\ nil, key_name, key_algorithm, opts \\ []) do
    with {:ok, namespace} <- Vault.create_owner_key_namespace(realm_name, user_id, key_algorithm) do
      opts = Keyword.put(opts, :namespace, namespace)
      Vault.create_keypair(key_name, key_algorithm, opts)
    end
  end

  def import_key(realm_name, user_id \\ nil, key_name, key_algorithm, key, opts \\ []) do
    with {:ok, namespace} <- Vault.create_owner_key_namespace(realm_name, user_id, key_algorithm) do
      opts = Keyword.put(opts, :namespace, namespace)
      Vault.import_key(key_name, key_algorithm, key, opts)
    end
  end

  def delete(realm_name, user_id \\ nil, key_name, key_algorithm) do
    with {:ok, namespace} <- Vault.owner_key_namespace(realm_name, user_id, key_algorithm),
         :ok <- Vault.enable_key_deletion(key_name, namespace: namespace) do
      Vault.delete_key(key_name, namespace: namespace)
    end
  end

  @doc false
  def create_namespace(realm_name, user_id \\ nil, key_algorithm) do
    with {:ok, algorithm} <- Core.key_type_to_string(key_algorithm),
         namespace_tokens = Core.namespace_tokens(realm_name, user_id, algorithm),
         {:ok, namespace} <- Core.create_nested_namespace(namespace_tokens),
         :ok <- Core.mount_transit_engine(namespace) do
      {:ok, namespace}
    end
  end

  @doc false
  def namespace_name(realm_name, user_id \\ nil, key_algorithm) do
    with {:ok, algorithm} <- Core.key_type_to_string(key_algorithm) do
      namespace =
        Core.namespace_tokens(realm_name, user_id, algorithm)
        |> Core.tokens_to_namespace()

      {:ok, namespace}
    end
  end
end
