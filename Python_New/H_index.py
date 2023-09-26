def hIndex(citations):

    if len(citations) == 0 : return 0 

    #if len(citations) == 1 : return 1 


    my_dict = {}

    left = 0 

    while left < len(citations):
        
         
        fixed = citations[left]
        print(f"fixed : {fixed}")
        if fixed != 0:
            if fixed not in my_dict:
                    my_dict[fixed] = 1
            
            for i in range(len(citations)):
                    
                    if citations[i] > fixed:
                        my_dict[fixed] += 1
        left += 1            

    result = []
    print(my_dict)
    for key,value in my_dict.items():
         if key <= value:
             result.append(key)
         else:
              result.append(value)    

    print(result)
    if len(result) > 0 : return max(result) 
    else : return 0 


if __name__ == "__main__":
    inputs = [[3,0,6,1,5],[1,3,1],[],[0],[100],[11,15],[1,2,2]]
    
    for i in range(len(inputs)):
      #  if i == 5:
            print(f"Test case : {i+1} ***Input = {inputs[i]}****************************** ")
            citations = inputs[i]
            print(f"Result : {hIndex(citations)}")
