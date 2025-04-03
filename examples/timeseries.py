import pandas as pd
from prophet import Prophet
import prophet.diagnostics
from timeit import default_timer as timer
from scaler import SchedulerClusterCombo, Client

def main():
    address = "tcp://127.0.0.1:2346"
    cluster = SchedulerClusterCombo(address=address, n_workers=6)
    client = Client(address=address)

    # Ensure that the client is connect before proceeding
    client.submit(lambda _: ..., None).result()

    # Load the data
    df = pd.read_csv(
        "https://raw.githubusercontent.com/facebook/prophet/master/examples/example_wp_log_peyton_manning.csv",
        parse_dates=["ds"]             
    )

    m = Prophet(daily_seasonality=False)
    m.fit(df)

    # this adapts the Scaler client to the Prophet diagnostics API
    class Adapter:
        def __init__(self, client: Client):
            self.client = client

        def map(self, func, *iterables):
            return self.client.map(func, [args for args in zip(*iterables)])

    start = timer()
    prophet.diagnostics.cross_validation(m, initial="730 days", period="180 days", horizon="365 days", parallel=None)
    non_parallel_time = timer() - start

    start = timer()
    prophet.diagnostics.cross_validation(m, initial="730 days", period="180 days", horizon="365 days", parallel=Adapter(client))
    parallel_time = timer() - start

    cluster.shutdown()

    print("-" * 30)
    print(f"Non-parallel time: {non_parallel_time:.2f}s")
    print(f"Parallel time: {parallel_time:.2f}s")
    print(f"Speedup: {non_parallel_time / parallel_time:.1f}x")

if __name__ == "__main__":
    main()
