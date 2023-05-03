class IncorrectConfigError(Exception):
    def __init__(self, message: str = None):
        super().__init__("""Config lacks 'schema', 'name' or both keys for the table to profile.
                         Example of config:
                         [
                            {
                                "schema": "UKI_DTM_SNU",
                                "name": "DIM_CUSTOMER",
                            }
                         ]""")
