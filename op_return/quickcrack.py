import base58

brokenWIF = input("input your borken WIF key: ")
base58chars = ['123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz']

for i in base58chars[0]:
    for j in base58chars[0]:
        for k in base58chars[0]:
            try:
                concatWIF = brokenWIF + i + j + k
                print(base58.b58decode_check(concatWIF))
            except:
                pass
