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

  def parse_file_ids(file_ids \\ "people") do
    File.read!(file_ids)
    |> String.split("\n")
    |> Enum.uniq()
    |> Enum.count()
    |> Enum.map(fn id ->
      if data = get_json(id) do
        File.write("results_ids.json", data <> "\n", [:append])
      end
    end)

    []
  end

  # Filter on types
  defp remove_end_tag(list),
    do:
      list
      |> Enum.reverse()
      |> tl()
      |> Enum.reverse()

  defp filter_on_ids(%{"id" => id}, ids), do: Enum.member?(ids, id)

  defp filter_on_type_text(list) when is_list(list), do: Enum.member?(list, {:string, "Q11424"})

  defp filter_on_type_text(_), do: false

  defp into_map(list),
    do:
      list
      |> Jaxon.Decoders.Value.decode()

  def get_json(id) do
    Tesla.get("https://www.wikidata.org/wiki/Special:EntityData/#{id}.json",
      query: [flavor: "full"]
    )
    |> case do
      {:ok, env} ->
        if env.status == 200 do
          env.body
          |> Jason.decode!()
          |> Map.get("entities")
          |> Map.get(id)
          |> Jason.encode!()
        end

      _ ->
        nil
    end
  end
end
