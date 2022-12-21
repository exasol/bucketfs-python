from typeguard import typechecked


class BucketFSConnectionConfig:
    """
    The BucketFSConnectionConfig contains all necessary information
    to connect to the BucketFS Server via HTTP[s]
    """

    @typechecked(always=True)
    def __init__(
        self, host: str, port: int, user: str, pwd: str, is_https: bool = False
    ):
        self._is_https = is_https
        if host == "":
            raise ValueError("Host can't be an empty string")
        self._host = host
        self._port = port
        if user not in [
            "w",
            "r",
        ]:  # The BucketFs currently supports only these two users
            raise ValueError(
                f"User can only be, 'w' (read-write access) or "
                f"'r' (read-only access), but got {user}"
            )
        self._user = user
        if pwd == "":
            raise ValueError("Password can't be an empty string")
        self._pwd = pwd

    @property
    def is_https(self) -> bool:
        return self._is_https

    @property
    def host(self) -> str:
        return self._host

    @property
    def port(self) -> int:
        return self._port

    @property
    def user(self) -> str:
        return self._user

    @property
    def pwd(self) -> str:
        return self._pwd
