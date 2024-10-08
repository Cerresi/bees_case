def load_replacements():
    """
    Loads a dictionary containing mappings of corrupted text to their corrected versions.

    This function is used to replace corrupted characters in text data, ensuring that the data is clean and accurate.

    Returns:
        dict: A dictionary where each key is a string with corrupted characters, and the corresponding value is the corrected string.
    """
    return {
        'Wimitzbr�u': 'Wimitzbräu',
        'K�rnten': 'Kärnten',
        'Dr.-Beurle-Stra�e': 'Dr.-Beurle-Straße',
        'Dr.-Beurle-Stra�e 1': 'Dr.-Beurle-Straße 1',
        'Feldkirchenstra�e 40': 'Feldkirchenstraße 40',
        'Caf� Okei': 'Cafe Okei',
        'Stiftstra�e 6': 'Stiftstraße 6',
        'Klagenfurt am W�rthersee': 'Klagenfurt am Wörthersee',
        'Mautner-Markhof-Stra�e 11': 'Mautner-Markhof-Straße 11',
        'Nieder�sterreich': 'Niederösterreich',
        'Anheuser-Busch Inc ̢���� Williamsburg': 'Anheuser-Busch Inc - Williamsburg',
        'Anheuser-Busch Inc â Newark': 'Anheuser-Busch Inc - Newark',
        'Anheuser-Busch Inc â Baldwinsville': 'Anheuser-Busch Inc - Baldwinsville',
        'Anheuser-Busch Inc â Cartersville': 'Anheuser-Busch Inc - Cartersville',
        'Anheuser-Busch Inc â Columbus': 'Anheuser-Busch Inc - Columbus',
        'Anheuser-Busch Inc â Fairfield': 'Anheuser-Busch Inc - Fairfield',
        'Anheuser-Busch Inc â Houston': 'Anheuser-Busch Inc - Houston',
        'Anheuser-Busch Inc â Jacksonville': 'Anheuser-Busch Inc - Jacksonville',
        'Anheuser-Busch Inc â Merrimack': 'Anheuser-Busch Inc - Merrimack',
        'Anheuser-Busch Inc â Newark': 'Anheuser-Busch Inc - Newark'
    }