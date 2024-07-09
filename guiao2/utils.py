def dht_hash(text, seed=0, maximum=2**10):
    """ FNV-1a Hash Function. """
    fnv_prime = 16777619
    offset_basis = 2166136261
    h = offset_basis + seed
    for char in text:
        h = h ^ ord(char)
        h = h * fnv_prime
    return h % maximum


def contains(begin, end, node):
    """Check node is contained between begin and end in a ring."""
    if begin < end:
        return begin < node <= end
    else:
        return node > begin or node <= end      
    # Como a peesquisa tem de ser feita em todo o anel, no caso em que os nós são apenas 3 e 7, é preciso procurar na metade do anel que não contém o 0 (]7, 0[) e na metade que o contém ([0, 3])
