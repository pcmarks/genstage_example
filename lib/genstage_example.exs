alias Experimental.GenStage

defmodule GenstageExample do
  @moduledoc """
  This is a simple example of setting up a data flow using the GenStage behaviour.
  It is based on the demo developed by the New York Times R&D team using their
  [Streamtools] (https://source.opennews.org/en-US/articles/introducing-streamtools/) graphical data flow builder.

  The flow begins by accessing a New York Citibike data feed. A list of bike
  stations is extracted. A particular bike station is pulled from this list.
  Finally, the json data about the bike station is printed to the console.
  """
  defmodule Map do
    @moduledoc """
    The Map stage supplies a string - a url - on demand.
    """
    use GenStage
    def init(url) do
      {:producer, url}
    end
    def handle_demand(demand, url) when demand > 0 do
      events = List.duplicate(url, demand)
      {:noreply, events, url}
    end
  end
  defmodule GetHTTP do
    @moduledoc """
    The GetHTTP stage expects a url string. It fetches the page. It assumes that
    the response is json-formatted. The json is converted to KeyMap
    """
    use GenStage
    def init(_) do
      {:producer_consumer, :ok}
    end
    def handle_events(events, _from, _state) do
      events = Enum.map(events, & retrieve_as_json &1)
      {:noreply, events, :ok}
    end
    defp retrieve_as_json(url) do
      {:ok, %HTTPoison.Response{body: body}} = HTTPoison.get(url)
      Poison.Parser.parse!(body)
    end
  end
  defmodule Unpack do
    @moduledoc """
    The Unpack stage expects a KeyMap event and uses the state string element
    as a key to extract a value.
    """
    use GenStage
    def init(element) do
      {:producer_consumer, element}
    end
    def handle_events(events, _from, element) do
      events = Enum.map(events, & &1[element])
      {:noreply, events, element}
    end
  end
  defmodule Filter do
    @moduledoc """
    The Unpack stage expects a list of KeyMaps. It filters this list for those
    elements that match a specified value.
    """
    use GenStage
    def init({filter, filter_value}) do
      {:producer_consumer, {filter, filter_value}}
    end
    def handle_events(events, _from, {filter, filter_value} = state) do
      events = Enum.map(events,
                           fn stations ->
                             Enum.filter(stations, & &1[filter] == filter_value)
                           end)
      {:noreply, events, state}
    end
  end
  defmodule Ticker do
    @moduledoc """
    This stage provides the demand to begin the flow of events
    """
    use GenStage
    def init(sleeping_time) do
      {:consumer, sleeping_time}
    end
    def handle_events(events, _from, sleeping_time) do
      IO.inspect(events)
      Process.sleep(sleeping_time)
      {:noreply, [], sleeping_time}
    end
  end
  # Start and intialize all of the stages
  {:ok, map}     = GenStage.start_link(Map, "http://feeds.citibikenyc.com/stations/stations.json")
  {:ok, getHTTP} = GenStage.start_link(GetHTTP, :ok)
  {:ok, unpack}  = GenStage.start_link(Unpack, "stationBeanList")
  {:ok, filter}  = GenStage.start_link(Filter, {"stationName", "W 14 St & The High Line"})
  {:ok, ticker}  = GenStage.start_link(Ticker, 5_000)
  # Connect all of the stages and start the flow of events with a single demand.
  GenStage.sync_subscribe(ticker, to: filter)
  GenStage.sync_subscribe(filter, to: unpack)
  GenStage.sync_subscribe(unpack, to: getHTTP)
  GenStage.sync_subscribe(getHTTP, to: map, max_demand: 1)

  Process.sleep(:infinity)
end
