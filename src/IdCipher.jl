module IdCipher

using YAML
using AES
using Random

export generate_key, encrypt_id, decrypt_id, encrypt_ids

function generate_key()
  return AES128Key(rand(UInt8, 16))
end

function pad_pkcs7(data::Vector{UInt8}, block_size::Int64)
  padding_length = block_size - (length(data) % block_size)
  padding = fill(UInt8(padding_length), padding_length)
  return vcat(data, padding)
end

function unpad_pkcs7(data::Vector{UInt8})
  padding_length = Int(data[end])
  return data[1:end-padding_length]
end

function encrypt_id(id::String, key::AES128Key)
  cipher = AESCipher(; key_length=128, mode=AES.CBC, key=key)
  iv = rand(UInt8, 16)
  id_bytes = Vector{UInt8}(id)
  padded_id = pad_pkcs7(id_bytes, 16)
  encrypted = encrypt(padded_id, cipher, iv)
  if encrypted isa AES.CipherText
    encrypted_data = encrypted.data
  else
    encrypted_data = encrypted
  end
  return bytes2hex(vcat(iv, encrypted_data))
end

function encrypt_ids(ids::Vector{String}, key::AES128Key)
  cipher = AESCipher(; key_length=128, mode=AES.CBC, key=key)
  encrypted_ids = Vector{String}(undef, length(ids))

  for (i, id) in enumerate(ids)
    iv = rand(UInt8, 16)
    id_bytes = Vector{UInt8}(id)
    padded_id = pad_pkcs7(id_bytes, 16)
    encrypted = encrypt(padded_id, cipher, iv)
    if encrypted isa AES.CipherText
      encrypted_data = encrypted.data
    else
      encrypted_data = encrypted
    end
    encrypted_ids[i] = bytes2hex(vcat(iv, encrypted_data))
  end

  return encrypted_ids
end

function decrypt_id(encrypted_id::String, key::AES128Key)
  cipher = AESCipher(; key_length=128, mode=AES.CBC, key=key)
  encrypted_data = hex2bytes(encrypted_id)
  iv = encrypted_data[1:16]
  ciphertext = encrypted_data[17:end]
  cipher_text = AES.CipherText{Vector{UInt8}, AES.CBC}(ciphertext, iv, length(ciphertext))
  decrypted = Vector{UInt8}(decrypt(cipher_text, cipher))
  unpadded_id = unpad_pkcs7(decrypted)
  return String(unpadded_id)
end

end
