def pluralize(word: str):
    """Return a plural form of a nown.
    >>> pluralize('object')
    'objects'
    >>> pluralize('entry')
    'entries'
    >>> pluralize('potato')
    'potatoes'
    >>> pluralize('banana')
    'bananas'
    >>>
    """
    match word.lower():
        case x if x.endswith("y"):
            return f"{x[:-1]}ies"
        case x if x.endswith("s") | x.endswith("o") | x.endswith("u"):
            return f"{x}es"
        case x:
            return f"{x}s"
