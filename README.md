# tap-ukg-workforce-ready
<a href="https://www.singer.io/">Singer</a> tap for extracting reports from UKG's Workforce Ready API. Package name shortened to `tap-ukg`.

UKG Workforce Ready API Documentation can be found <a href="https://secure.saashr.com/ta/docs/rest/public/#">here</a>.

### Getting Started
Your config file must include four parameters:
- `api_key` - your unique API key for the UKG Workforce Ready API
- `company` - this is your company shorthand ID
- `username` - your UKG username
- `password` - your UKG password

### Catalog Discovery
`tap-ukg --config config.json --discover > catalog.json`

### Running the Tap
`tap-ukg --config config.json --catalog catalog.json`