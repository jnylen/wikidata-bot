defmodule WikidataBot do
  def parse_file do
    file = File.stream!("results.json", [:append])

    Application.get_env(:wikidata_bot, :file_path, "20201026.json.gz")
    |> File.stream!([:trim_bom, :compressed])
    |> Jaxon.Stream.from_enumerable()
    |> Stream.filter(&filter_on_type_text/1)
    |> Stream.map(&remove_end_tag/1)
    |> Jaxon.Stream.query([:root])
    |> Stream.map(&Jason.encode!/1)
    |> Stream.map(fn x ->
      "\n" <> x
    end)
    |> Stream.into(file)
    |> Stream.run()

    File.close(file)

    []
  end

  # Filter on types
  defp remove_end_tag(list),
    do:
      list
      |> Enum.reverse()
      |> tl()
      |> Enum.reverse()

  defp filter_on_type_text(list) when is_list(list), do: Enum.member?(list, {:string, "Q11424"})

  defp filter_on_type_text(_), do: false

  defp into_map(list),
    do:
      list
      |> Jaxon.Decoders.Value.decode()
end
