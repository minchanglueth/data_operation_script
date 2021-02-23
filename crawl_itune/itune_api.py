class ItunesUrl:
    ALBUM_TRACKS_WITH_COUNTRY = "https://itunes.apple.com/lookup?id={}&entity=song&country={}&limit=1000"
    ALBUM_TRACKS = "https://itunes.apple.com/lookup?id={}&entity=song&limit=1000"
    SEARCH_BY_ID = "https://itunes.apple.com/lookup?id={}"
    SEARCH_BY_ID_AND_COUNTRY = "https://itunes.apple.com/lookup?id={}&country={}"
    ALBUM_URL_WITH_COUNTRY = "https://music.apple.com/{}/album/{}"
    ARTIST_URL_WITH_COUNTRY = "https://music.apple.com/{}/artist/{}"
    ARTIST_ALBUMS_WITH_COUNTRY = "https://itunes.apple.com/lookup?id={}&entity=album&country={}&limit=1000"
    SEARCH_ARTIST_BY_NAME = (
        "https://itunes.apple.com/search?term={}&entity=allArtist&attribute=allArtistTerm&limit=1000"
    )