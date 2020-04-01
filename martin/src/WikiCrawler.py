import asyncio
from collections import defaultdict, deque
from collections.abc import Iterable
from typing import List, Tuple, Generator
from typing import Union

import aiohttp
from bs4 import BeautifulSoup as Soup
from bs4.element import Tag
from robot.api import logger
from yarl import URL

StrOrUrl = Union[str, URL]


class WikiCrawler:
    def __init__(self, start: StrOrUrl, target: StrOrUrl, *, concurrent: int = 25, keywords: List[str] = None) -> None:
        name = type(self).__name__
        logger.debug(f'Initializing {name}')
        logger.debug(f'{name} will start at "{start}" and will stop at "{target}"')
        logger.debug(f'{name} will use {concurrent} concurrent tasks')

        if keywords is not None:
            to_print = '\n'.join([f'\t{num}) {kw}' for num, kw in enumerate(keywords)])
            logger.debug(f'{name} prioritizing links by keywords:\n{to_print}')
            self._keywords = keywords
        else:
            self._keywords = []

        self._start = URL(start)
        self._target = URL(target)
        self._queue = asyncio.PriorityQueue()
        self._graph = defaultdict(set)
        self._session = aiohttp.ClientSession()
        self._semaphore = asyncio.Semaphore(concurrent)
        self._concurrent = concurrent
        self._tasks = []
        self._target_found = asyncio.Event()
        logger.debug(f'{type(self).__name__} initialization complete')

    def __call__(self) -> List[str]:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self._first(loop))
        return self.shortest_path()

    def shortest_path(self) -> List[str]:
        start = str(self._start)
        target = str(self._target)
        dist = {start: [start]}
        q = deque()
        q.append(start)
        while len(q):
            at = q.popleft()
            for next_ in self._graph[at]:
                if next_ not in dist:
                    dist[next_] = [dist[at], next_]
                    q.append(next_)
        return list(_flatten(dist.get(target)))

    async def _first(self, loop) -> None:
        async with self._session.get(self._start) as response:
            if response.content_type != 'text/html':
                logger.error(f'Initial fetch failed: {self._start} not HTML')
                return
            links = _get_article_links(await response.text())
        await self._process_links(links, self._start)
        await asyncio.gather(*[asyncio.create_task(self._worker()) for _ in range(self._concurrent)], loop=loop)
        if not self._session.closed:
            await self._session.close()

    async def _process_links(self, links: List[Tag], current: URL) -> None:
        for priority, url in _prioritize_by_keyword(links, self._keywords):
            url = current.join(url)
            self._graph[str(current)].add(str(url))
            if url == self._target:
                self._target_found.set()
                break

            await self._queue.put((priority, url))

    async def _worker(self) -> None:
        while not self._target_found.is_set():
            _, current = await self._queue.get()
            if str(current) not in self._graph:
                async with self._semaphore, self._session.get(current) as response:
                    if response.content_type == 'text/html':
                        links = _get_article_links(await response.text())
                    else:
                        logger.debug(f'Fetch "{current}" failed, not HTML')
                        continue

                logger.debug(f'Fetch "{current}"... found {len(links)} article links')
                await self._process_links(links, current)
            self._queue.task_done()


def _flatten(iterable):
    for item in iterable or []:
        if isinstance(item, Iterable) and not isinstance(item, (str, bytes)):
            yield from _flatten(item)
        else:
            yield item


def _prioritize_by_keyword(links: List[Tag], keywords: List[str] = None) -> Generator[Tuple[int, URL], None, None]:
    keywords = keywords or []
    for link in links:
        text = link.text.lower()
        href = link['href']
        for index, word in enumerate([kw.lower() for kw in keywords]):
            if word in text or word in href.lower():
                yield index, URL(href)
                break
        else:
            yield len(keywords), URL(href)


def _get_article_links(source: str) -> List[Tag]:
    html = Soup(source)
    return [link for link in html.find_all('a', href=lambda x: x and x.startswith('/wiki/'))]


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('start', type=URL)
    parser.add_argument('target', type=URL)
    parser.add_argument('-c', '--concurrent', type=int)
    parser.add_argument('-k', '--keywords', action='append')
    args = parser.parse_args()

    crawler = WikiCrawler(args.start, args.target, concurrent=args.concurrent, keywords=args.keywords)
    print(' '.join(crawler()))
