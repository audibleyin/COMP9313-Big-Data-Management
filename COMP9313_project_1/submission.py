def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    CandidatesNum = -1

    def diff(data_hash, query_hash):
        diff_result = list()
        for i in range(len(data_hash)):
            res = abs(data_hash[i] - query_hash[i])
            diff_result.append(res)
        return diff_result

    data_hashes = data_hashes.map(lambda e: (e[0], diff(e[1], query_hashes), False))
    while CandidatesNum < beta_n:
        offset += 1 #(0)
        # e : (id, difference)
        candidatesRDD = data_hashes.flatMap( lambda e : [e[0]] if ifMatched(e[1], alpha_m, offset) else [])

        CandidatesNum = candidatesRDD.count()

    return candidatesRDD

def ifMatched(difference, alpha_m, offset):
    count = 0
    i = 0
    while i < len(difference):
        if difference[i] <= offset:
            count += 1
        if count >= alpha_m:
            return True
        i += 1
    return False
