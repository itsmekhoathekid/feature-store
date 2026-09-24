from pyflink.common import Types


def event_type():
    return Types.TUPLE([Types.STRING(), Types.STRING(), Types.DOUBLE(), Types.LONG()])


EVENT_TYPE = event_type()
ACCUMULATOR_TYPE = Types.TUPLE([Types.DOUBLE(), Types.LONG()])
FEATURE_TYPE = Types.TUPLE(
    [
        Types.STRING(),
        Types.DOUBLE(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
    ]
)

# Tuple indexes are named here so the data contract stays explicit in operator code.
EVENT_ID = 0
CUSTOMER_ID = 1
AMOUNT = 2
EVENT_TIMESTAMP_MS = 3
