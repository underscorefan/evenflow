from typing import TypeVar, Generic, Callable, Optional

L = TypeVar('L')
R = TypeVar('R')
T = TypeVar('T')


class Either(Generic[L, R]):
    def __init__(self, left: Optional[L], right: Optional[R]):
        self.__left = left
        self.__right = right

    def map(self, callback: Callable[[R], T]) -> 'Either[L, T]':
        if self.__right:
            return Either(self.__left, callback(self.__right))
        return self

    def flat_map(self, callback: Callable[[R], 'Either[L, T]']) -> 'Either[L, T]':
        if self.__right:
            return callback(self.__right)
        return self

    def on_left(self, callback: Callable[[L], T]) -> Optional[T]:
        if self.__left:
            return callback(self.__left)
        return None

    def on_right(self, callback: Callable[[R], T]) -> Optional[T]:
        if self.__right:
            return callback(self.__right)
        return None


class Left(Generic[L], Either[L, None]):
    def __init__(self, left: L):
        super().__init__(left, None)


class Right(Generic[R], Either[None, R]):
    def __init__(self, right: R):
        super().__init__(None, right)
