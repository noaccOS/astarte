ALTER TABLE :keyspace.ownership_voucher
ADD (
  replacement_guid blob,
  replacement_rendezvous_info blob,
  replacement_public_key blob,
  key_name varchar,
  user_id blob,
);
