from typing import List, Dict


class QueryManager:
    def __init__(self, columns: List[str], table: str):
        self.columns = columns
        self.table = table

    def make_insert(self) -> str:
        insert_stub = 'INSERT INTO {} ({}) VALUES ({})'
        placeholders = ['${}'.format(i) for i, _ in enumerate(self.columns, 1)]
        return insert_stub.format(self.table, self.__join(self.columns), self.__join(placeholders))

    def sort_args(self, args: Dict[str, str]):
        return [args[col] for col in self.columns]

    @staticmethod
    def __join(elements: List[str]):
        return ', '.join(elements)
