@dp.table(
    name="events_silver",
    comment="Running count per user with inactivity timeout",
    table_properties={
        "quality": "silver"
    }
)
def events_silver():

    events = spark.table("events_bronze")

    from pyspark.sql.streaming import GroupState, GroupStateTimeout
    from pyspark.sql import Row

    class UserState:
        def __init__(self):
            self.running_count = 0

    def stateful_count(user_id, events_iter, state: GroupState):

        if state.hasTimedOut:
            final_state = state.get()
            state.remove()
            return [
                Row(
                    user_id=user_id,
                    running_count=final_state.running_count,
                    status="expired"
                )
            ]

        user_state = state.get() if state.exists else UserState()

        for _ in events_iter:
            user_state.running_count += 1

        state.update(user_state)

        state.setTimeoutDuration("30 minutes")
        return [
            Row(
                user_id=user_id,
                running_count=user_state.running_count,
                status="active"
            )
        ]

    return (
        events
        .groupBy("user_id")
        .flatMapGroupsWithState(
            func=stateful_count,
            outputMode="update",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
            stateType=UserState
        )
    )
