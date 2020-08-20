def update(l,k,table):
    if l == 1:
        table[l][k] = k * B -2 * table[2][k] - 3 * table[3][k] - 4 * table[4][k] - 5 * table[5][k]
        return
    table[l][k] = ((6 - l) * table[l - 1][k - 1]) / (N - k + 1) + table[l][k - 1] - ((6 - l - 1) * table[l][k - 1]) / (N - k + 1)
    return

R,N,K= 20000000, 500, 200
B = R / N

table = [[0 for _ in range(201)] for _ in range(6)]
table[0][0]=B

tmp = [5,4,3,2,1]
for k in range(201):
    for l in tmp:
        update(l, k, table)

print(table[5][200])