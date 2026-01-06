from pyspark import pipelines as dp
from pipelines.stateful_processor import StatefulProcessor, StatefulProcessorHandle

@dp.table(
    name="events_silver",
    table_properties={
        "quality": "silver"
    }
)
def events_silver():

    events = spark.table("events_bronze")

    class UserState:
        def __init__(self):
            self.running_count = 0

    def process_user(user_id, events_iter, state: StatefulProcessorHandle):

        if state.hasTimedOut():
            final_state = state.get()
            state.remove()
            return [{"user_id": user_id, "running_count": final_state.running_count, "status": "expired"}]

        user_state = state.get() if state.exists() else UserState()

        for _ in events_iter:
            user_state.running_count += 1

        state.update(user_state)
        state.setTimeoutMinutes(30)

        return [{"user_id": user_id, "running_count": user_state.running_count, "status": "active"}]

    processor = StatefulProcessor(
        input_df=events,
        key_column="user_id",
        state_class=UserState,
        output_mode="update",
        process_func=process_user
    )

    return processor.run()
