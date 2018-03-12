// Functions

// // 1) check for single even
def isEven(num: Int) = num % 2 == 0

// 2) check if there is an even number in a List
def checkEvenInList(numList: List[Int]): Boolean = {
    for (i <- numList) {
        if (i % 2 == 0) {
            return true
        }
    }
    return false
}

// 3) Lucky number Seven
def sumSevenTwice(numList: List[Int]): Int = {
    var sum = 0
    for (i <- numList) {
        sum += i
        if (i == 7) {
            sum += i
        }
    }
    return sum
}

// 4) can you balance
def canBalance(numList: List[Int]): Boolean = {
    val sum = numList.sum
    if (sum % 2 == 1) {
        return false
    }
    val half = sum / 2
    var halfSum = 0

    for (num <- numList) {
        halfSum += num
        if (halfSum == half) {
            return true
        }
    }
    return false
}

// 5) Palindorme check
def isPalindrome(str: String): Boolean = {
    if (str == str.reverse) {
        return true
    }
    return false
}

println(isEven(4))
println(isEven(3))
// val testList = List(7,3,4)
// val testStr = "aa"
// val result = isPalindrome(testStr)

// println(result)
