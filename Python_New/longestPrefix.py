def longestCommonPrefix(strs):
    result = ""

    for i in range(len(strs[0])):
        for s in strs:
            if i == len(s) or s[i] != strs[0][i]:
                return result 
        result += s[i]     

    return result 




if __name__ == "__main__":
    inputs = [["flower","flow","flight"],["dog","racecar","car"]]
    for strs in inputs: 
        print(f"{strs} --> {longestCommonPrefix(strs)} ")

