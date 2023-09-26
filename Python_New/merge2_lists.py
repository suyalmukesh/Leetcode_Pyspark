"""
Input: nums1 = [1,2,3,0,0,0], m = 3, nums2 = [2,5,6], n = 3
Output: [1,2,2,3,5,6]
Explanation: The arrays we are merging are [1,2,3] and [2,5,6].
The result of the merge is [1,2,2,3,5,6] with the underlined elements coming from nums1."""

def shift(nums,m,n,k):
    print("Entered here ")
    for i in range(m+n-1,k,-1):
        nums[i], nums[i-1] = nums[i-1], nums[i]
        print(f"nums : {nums}")
    return nums

def merge(nums1, m, nums2, n):
    
    nn = m+n-1
    j = 0 
    for i in range(nn):
        print(i,j)
        if j < n:
            if nums1[i] >= nums2[j]:
                nums1 = shift(nums1,m,n,i) 
                nums1[i]=nums2[j]
                j+=1
            else:
                j+=1
    return nums1        

if __name__ == "__main__":
    nums1 = [1,2,3,0,0,0]
    m = 3
    nums2 = [2,5,6]
    n = 3

    print(merge(nums1, m, nums2, n))