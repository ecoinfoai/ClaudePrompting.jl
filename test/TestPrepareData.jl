using Test
using CSV
using DataFrames
using YAML
using OrderedCollections
using Dates

import ClaudePrompting.IdCipher as IC
import ClaudePrompting.PrepareData as PD

@testset "Multiple IDs encryption test" begin

  df = DataFrame(
    "IDs" => [2024194999, 2024194998],
    "Textdata" => ["Some text here", "Other text here"]
  )
  ids_vec = string.(df[!, :IDs])

  key = IC.generate_key()
  encrypted_df = PD.encrypt_input_data(df, key)
  @test length(encrypted_df[!, :IDs]) == 2

  encrypted_ids = encrypted_df[!, :IDs] 
  decrypted_ids = [IC.decrypt_id(id, key) for id in encrypted_ids]
  @test decrypted_ids == ids_vec
end

@testset "DataFrame to ordered_dict test" begin
  @testset "Functionality test" begin 
    df1 = DataFrame(A = [1, 2], B = ["x", "y"]) 
    key_order1 = [:A, :B]
    res = PD.df_to_ordered_dicts(df1, key_order1)
    @test collect(keys(res[1])) == [:A, :B]
    @test res[1][:A] == 1
  end
  @testset "Data type handling test" begin
    df2 = DataFrame(
      Int = [1],
      Float = [3.14],
      String = ["test"],
      Date = [Date(2023, 1, 1)],
      Bool = [true]
    )
    key_order2 = [:Int, :Float, :String, :Date, :Bool]
    res2 = PD.df_to_ordered_dicts(df2, key_order2)
    @test res2[1][:Int] isa Int 
    @test res2[1][:Float] isa Float64 
    @test res2[1][:String] isa String 
    @test res2[1][:Date] isa Date
    @test res2[1][:Bool] isa Bool 
  end
  @testset "Empty DataFrame processing test" begin
    df_empty = DataFrame(A = Int[], B = String[])
    res_empty = PD.df_to_ordered_dicts(df_empty, [:A, :B])
    @test isempty(res_empty)
  end
  @testset "Large Data Performance" begin
    large_df = DataFrame(
      A = 1:10000, B = rand(10000), C = ['a':'z'...][rand(1:26, 10000)]
    )
    @time result_large = PD.df_to_ordered_dicts(large_df, [:A, :B, :C])
    @test length(result_large) == 10000
  end
end
