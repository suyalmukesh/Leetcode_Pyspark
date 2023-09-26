def longestConsecutive_(nums):
    nums = sorted(nums)

    out = []
    temp = [nums[0]]


    for i in range(1,len(nums)):
        if (nums[i] - nums[i-1] == 0):
            pass  

        elif (nums[i] - nums[i-1] == 1):
            temp.append(nums[i])    
        else:
            out.append(temp)
            temp = []
            temp.append(nums[i])
    out.append(temp)
    return max([len(x) for x in out])



def longestConsecutive(nums):
        nums = list(set(nums))
        nums = sorted(nums)

        out = []
        temp = []

        if len(nums) > 0 : temp = [nums[0]]
        else: temp  

        for i in range(1,len(nums)):
            if nums[i] - nums[i-1] != 1 :
                out.append(temp)
                temp = []
                temp.append(nums[i])
            else:
                temp.append(nums[i])
        out.append(temp)        
        
        return max([len(x) for x in out])


if __name__ == "__main__":
    inputs = [[100,4,200,1,3,2],[0,3,7,2,5,8,4,6,0,1]]
    for i in range(len(inputs)):
        nums = inputs[i]
        print(longestConsecutive(nums))
        print(longestConsecutive_(nums))