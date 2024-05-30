import os

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from typing import Callable, Collection, List


class ParallelFunctionExecutor:
    def __init__(self, func: Callable, collection: Collection, *args, **kwargs) -> None:
        """
        Класс для параллельного запуска функции в заданном количестве процессов и потоков.
        :param func: параллельно запускаемая функция.
        :param collection: коллекция объектов для итерации по ним.
        :param *args: другие позиционные аргументы.
        :param **kwargs: другие именованные аргументы.
        :return: None
        """
        self.func = func
        self.collection = collection
        self.args = args
        self.kwargs = kwargs

    def _create_list_of_subcollections(self, parts: int) -> List[List]:
        """
        Разделить коллекцию объектов для итерации на подколлекции.
        :param parts: количество подколлекций.
        :return: список подколлекций.
        """
        if len(self.collection) > parts:
            return [self.collection[i::parts] for i in range(parts)]
        return [[i] for i in self.collection]

    def _create_threads(self, subcollection: List, thread_count: int = None) -> List:
        """
        Создать потоки для элементов подколлекций для итерации по ним.
        :param subcollection: подколлекция для итерации.
        :param thread_count: количество потоков.
        :return: список результатов выполнения переданной функции.
        """
        results = []
        with ThreadPoolExecutor(thread_count) as executor:
            future_list = []
            for obj in subcollection:
                future = executor.submit(self.func, obj, *self.args, **self.kwargs)
                future_list.append(future)
            for f in future_list:
                results.append(f.result())
        return results

    def run(self, cpu_count: int = os.cpu_count(), thread_count: int = 5) -> List:
        """
        Создать процессы и в них потоки для элементов коллекций.
        :param cpu_count: количество процессов.
        :param thread_count: количество потоков для каждого процесса.
        :return: список результатов выполнения переданной функции.
        """
        results = []
        list_of_iter_objs = self._create_list_of_subcollections(cpu_count)
        with ProcessPoolExecutor() as executor:
            future_list = []
            for list_of_objs in list_of_iter_objs:
                future = executor.submit(self._create_threads, list_of_objs, thread_count)
                future_list.append(future)
            for f in future_list:
                results.append(f.result())
        return [i for r in results for i in r]
