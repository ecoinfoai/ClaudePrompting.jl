module IdCipher

using YAML
using AES
using Random

export generate_key, encrypt_id, decrypt_id, save_key, load_key

"""
    generate_key()::AES128Key

Generate a new AES-128 key.
"""
function generate_key()::AES128Key
  return AES128Key(rand(UInt8, 16))
end

"""
    save_key(key::AES128Key, filepath::String)

Save an AES key to a YAML file in hex format.
"""
function save_key(key::AES128Key, filepath::String)
  key_hex = bytes2hex(key.data)
  YAML.write_file(filepath, Dict("key" => key_hex))
end

"""
    load_key(filepath::String)::AES128Key

Load an AES key from a YAML file.
"""
function load_key(filepath::String)::AES128Key
  data = YAML.load_file(filepath)
  key_hex = data["key"]
  return AES128Key(hex2bytes(key_hex))
end

function pad_pkcs7(data::Vector{UInt8}, block_size::Int64)::Vector{UInt8}
  padding_length = block_size - (length(data) % block_size)
  padding = fill(UInt8(padding_length), padding_length)
  return vcat(data, padding)
end

function unpad_pkcs7(data::Vector{UInt8})::Vector{UInt8}
  padding_length = Int(data[end])
  return data[1:end-padding_length]
end

"""
    encrypt_id(id::String, key::AES128Key)::String

Encrypt a string ID using AES-128 CBC.
"""
function encrypt_id(id::String, key::AES128Key)::String
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

"""
    decrypt_id(encrypted_id::String, key::AES128Key)::Union{String, Nothing}

Decrypt an encrypted ID string. Returns `nothing` if decryption fails.
"""
function decrypt_id(encrypted_id::String, key::AES128Key)::Union{String, Nothing}
  try
    cipher = AESCipher(; key_length=128, mode=AES.CBC, key=key)
    encrypted_data = hex2bytes(encrypted_id)
    iv = encrypted_data[1:16]
    ciphertext = encrypted_data[17:end]
    cipher_text = AES.CipherText{Vector{UInt8}, AES.CBC}(ciphertext, iv, length(ciphertext))
    decrypted = Vector{UInt8}(decrypt(cipher_text, cipher))
    unpadded_id = unpad_pkcs7(decrypted)
    return String(unpadded_id)
  catch e
    println("Error decrypting ID: $e")
    return nothing
  end
end

end