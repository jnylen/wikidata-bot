defmodule WikidataBot.Client do
  use Tesla

  plug(Tesla.Middleware.BaseUrl, Application.get_env(:metagraph_sdk, :url))

  plug(Tesla.Middleware.Headers, [
    {"authorization", "Bearer " <> Application.get_env(:metagraph_sdk, :token)}
  ])

  plug(Tesla.Middleware.JSON, engine: Jason)

  def create_item(data, type) when is_map(data), do: create_item(type, data)

  def create_item(type, data) when is_map(data) do
    post(
      "/custom/create",
      %{
        "type" => type,
        "values" => data
      }
    )
  end

  def update_item(data, uid) when is_map(data), do: update_item(uid, data)

  def update_item(uid, data) when is_map(data) do
    post(
      "/custom/update",
      %{
        "uid" => uid,
        "values" => data
      }
    )
  end
end
