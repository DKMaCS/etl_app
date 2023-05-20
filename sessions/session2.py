import math


def find_next_prime(num):
    """ Finds the next highest prime

    param num:int
        starting integer for finding next prime
    return: int
        next prime number from num
    """
    def is_prime(potentially_prime):
        """ Determines if potentially_prime is prime

        param potentially_prime: int
            number undergoing primality test
        return: bool
            True if potentially_prime is prime; False otherwise
        """
        # Https://primes.utm.edu/notes/faq/six.html
        # Primality test based on prime number characteristics
        divisor = 5
        last_divisor = int(math.sqrt(potentially_prime))

        if potentially_prime <= 3:
            if potentially_prime >= 2:
                return True
            else:
                return False
        if not potentially_prime % 2 or not potentially_prime % 3:
            return False

        while divisor <= last_divisor:
            if not potentially_prime % divisor or \
               not potentially_prime % (divisor + 2):
                return False
            divisor += 6
        return True

    # Iterating through numbers from num until prime found
    while True:
        num += 1
        if is_prime(num):
            return num


def flip_variables(first_num, second_num):
    """ Swaps variable assignments

    param first_num: int
        first number to swap
    param second_num: int
        second number to swap
    return: int, int
        function returns the swapped variables
    """
    first_num, second_num = second_num, first_num
    return first_num, second_num


def is_palindrome(phrase):
    """ Tests if 'phrase' value is a palindrome

    param phrase: str
        string for palindrome test
    return: bool
        True is 'phrase' is palindrome; False otherwise
    """
    def filter_out_invalid_chars(pre_phrase):
        """ Filters non-alphanumeric characters from original phrase

        param pre_phrase: str
            phrase to filter of invalid characters
        return: list of strings
            returns a list of valid, lower cased letters from phrase
        """
        valid_chars = []
        for letter in pre_phrase:
            if letter.isalnum():
                valid_chars.append(letter.lower())
        return valid_chars

    list_of_valid_chars = filter_out_invalid_chars(phrase)
    left_char_idx, right_char_idx = 0, len(list_of_valid_chars) - 1

    while left_char_idx <= right_char_idx:
        # Comparing letters from outside (left and right chars) in
        left_char = list_of_valid_chars[left_char_idx]
        right_char = list_of_valid_chars[right_char_idx]
        if left_char != right_char:
            return False

        left_char_idx += 1
        right_char_idx -= 1

    return True


def fizz_buzz():
    """ Returns the correct phrase based on divisibility

    return: None
        Prints strings 'Fizzbuzz', 'Fizz', 'Buzz', or a num in str form
    """
    for i in range(1, 101):
        if i % 3 == 0 and i % 5 == 0:
            print('FizzBuzz')
        elif i % 3 == 0:
            print('Fizz')
        elif i % 5 == 0:
            print('Buzz')
        else:
            print(str(i))


def pretty_print(n, char):
    """ Prints a nice visual to console

    param n: int
        number of total rows
    param char: str
        the character to use in illustration
    return: None
        prints the visual so returns nothing
    """
    for row in range(n):
        if n < 0:
            print("Invalid entry. Positive, natural numbers only.")
        elif row == 0 or row == n - 1:
            print(char * n)
        else:
            # Note: this conditional branch was created to match
            # Examples in class ppt
            if char == "*":
                print(char + (n - 3) * " " + char)
            else:
                print(char + (n - 1) * " " + char)


def count_perfect_squares(nums):
    """ Counts number of perfect squares in given list of ints (nums)

    param nums: list of ints
        list of integers to traverse
    return: int
        number of perfect square pairs
    """
    def is_perfect_square(first_num, second_num):
        """ Determines if a pair is also a perfect square
        
        param first_num: int
            first number in pair
        param second_num: int
            second number in pair
        return: bool
            True if pair is perfect square;  False otherwise
        """
        root_squared_sum = (first_num ** 2 + second_num ** 2) ** .5
        return math.ceil(root_squared_sum) == math.floor(root_squared_sum)

    ct = 0
    unique_combos = []

    for idx, num in enumerate(nums):
        for num_pairing in nums[idx + 1:]:
            # Going through each pair/does not consider past comparisons
            sorted_pair = sorted([num, num_pairing])
            if is_perfect_square(num, num_pairing) and \
               sorted_pair not in unique_combos:
                ct += 1
                unique_combos.append(sorted_pair)
    return ct


if __name__ == '__main__':
    print("Testing find_next_prime:")
    seen = set()
    for z in range(0, 100):
        np = find_next_prime(z)
        seen.add(np)
    print(sorted(seen))
    print()
    
    print("Testing flip_variables:")
    a = 14
    b = 41
    print(f'Before switch: a = {a} and b = {b}')
    a, b = flip_variables(a, b)
    print(f'After switch: a = {a} and b = {b}')
    print()
    
    print("Testing fizz_buzz:")
    fizz_buzz()
    print()
    
    print("Testing count_perfect_squares:")
    print(count_perfect_squares([-4, -1, 7, 3, -5, 6, -2, 8, -4, -8, 6, -6]))
    print()

    print("Testing is_palindrome:")
    phrases = ["asdffdsa", "asdfdsa", "a_sd ffd;sa", "asdfgrfdsa", " ", "   ", "asdfgdaa", "kayak", "level",
               'Wo, Nemo! Toss a lasso to me now!']
    for each_phrase in phrases:
        print(is_palindrome(each_phrase))
    print()

    print("Testing pretty_print:")
    pretty_print(10, "*")
    pretty_print(7, "-")
    print()