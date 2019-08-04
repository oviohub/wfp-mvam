""" A set of utils for strings handling and cleaning. """

# KOBO prefixes to remove by default.
KOBO_PREFIXES = ['a/', 'b/', 'c/', 'd/', 'e/', 'f/',
                 's1/', 's2/', 's3/', 's4/',
                 'group_ou3kf64/', 'group_eb4vh34/', 'group_hj0gx41/',
                 ]


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text


def remove_prefix_list(text, prefixes=KOBO_PREFIXES):
    """
        Remove prefix given a text and a list of prefixes.
    """
    for prefix in prefixes:
        if text.startswith(prefix):
            text = text[len(prefix):]

    return text


def choice_selected(choice, choices):
    return str(choice) in str(choices)


def force_inital_zeros(number, expected_length):
    number_as_string = str(int(number))
    missing_length = expected_length - len(number_as_string)
    return missing_length * "0" + number_as_string
