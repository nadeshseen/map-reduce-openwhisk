def main(args):
    tokens = args.get("tokens")
    # print(tokens)
    return_dict = {}
    for token in tokens:
        if token in return_dict:
            return_dict[token]+=1
        else:
            return_dict[token]=1
    print(return_dict)
    return return_dict
# main({"tokens": "nadesh"})