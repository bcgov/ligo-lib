class UnionFind:
    """
    Union-Find with path compression implementation
    """
    size = 0
    parent = []
    weight = []

    def __init__(self, n):
        """
        Creates n disjoint sets , one per each element
        :param n: Total number of elements
        :return:
        """
        self.size = n
        self.parent = list(range(n))
        self.weight = [0] * n

    def count(self):
        """
        :return: Total number of disjoint sets.
        """
        return self.size

    def find(self, x):
        """
        Finds the root of the set that element x belons to.
        :param x: input element
        :return: the root of the set containing x
        """
        i = x

        while i != self.parent[i]:
            self.parent[i] = self.parent[self.parent[i]]
            i = self.parent[i]

        return i

    def linked(self, x, y):
        """
        Checks if both x and y are in the same set.
        :param x:
        :param y:
        :return: True if x and y are in the same set, False otherwise
        """
        return self.find(x) == self.find(y)

    def union(self, x, y):
        """
        Joins the two disjont sets that contain x and y
        :param x:
        :param y:
        :return:
        """
        i = self.find(x)
        j = self.find(y)

        # Nothing to do if x and y are in the same set.
        if i == j:
            return

        if self.weight[i] < self.weight[j]:
            self.parent[i] = j
        elif self.weight[i] > self.weight[j]:
            self.parent[j] = i
        else:
            self.parent[j] = i
            self.weight[i] += 1

        self.size -= 1
