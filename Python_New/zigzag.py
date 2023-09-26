def convert(s,numRows):
    if numRows == 1: return s 

    ind = bool(0)
    i = 0 

    ans = [str("") for x in range(numRows)]
    zigzag = ""

    for char in s:
        ans[i] += char 

        if (i == 0) or (i == numRows - 1):
            ind = not ind 

        if ind:
            i += 1
        else:
            i -= 1 

    for string in ans:
        zigzag += string 
        
    return zigzag      









if __name__ == "__main__":
    inputs = ["PAYPALISHIRING","PAYPALISHIRING"]
    numRows = [3,4]
    Expected = ["PAHNAPLSIIGYIR","PINALSIGYAHRPI"]

    for i in range(len(inputs)):
        s,rows = inputs[i],numRows[i]
        output = convert(s,rows)
        if output == Expected[i]:
            print("Test Case Passed")
        else:
            print("Test Case Failed")     

