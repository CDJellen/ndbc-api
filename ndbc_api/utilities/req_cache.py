class RequestCache:
    class Request:

        __slots__ = 'k', 'v', 'next', 'prev'

        def __init__(self, request: str, response: dict):
            self.k = request
            self.v = response
            self.next = self.prev = None

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.cache = dict()
        self.left = RequestCache.Request('$', '$')
        self.right = RequestCache.Request('$', '$')
        self.left.next = self.right
        self.right.prev = self.left

    def remove(self, node: Request) -> None:
        node.prev.next = node.next
        node.next.prev = node.prev

    def add(self, node: Request):
        node.prev = self.right.prev
        node.next = self.right
        self.right.prev.next = node
        self.right.prev = node

    def get(self, request: str) -> dict:
        if request in self.cache:
            self.remove(self.cache[request])
            self.add(self.cache[request])
            return self.cache[request].v
        else:  # request not made before
            return dict()

    def put(self, request: str, response: dict) -> None:
        if request in self.cache:
            self.remove(self.cache[request])

        self.cache[request] = RequestCache.Request(request, response)
        self.add(self.cache[request])

        if len(self.cache) > self.capacity:
            to_remove = self.left.next
            self.remove(to_remove)
            del self.cache[to_remove.k]
