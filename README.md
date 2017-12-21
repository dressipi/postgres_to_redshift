# PostgresToRedshift

This gem copies data from postgres to redshift. It's especially useful to copy data from postgres to redshift in heroku.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'postgres_to_redshift'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgres_to_redshift

## Usage

Set your source and target databases, as well as your s3 intermediary.
If POSTGRES_TO_REDSHIFT_MIGRATE set to true, that means a full copy of the original postgres database, otherwise just schemas and tables.

```bash
export REDSHIFT_URI='postgres://username:password@host:port/database-name'
export PGHOST=
export PGUSER=
export PGPASSWORD=
export PGDATABASE=

# if view manager is not live yet and we want to copy the views as well in order to test Rspec in Warehouse-Analysis:
export VIEW_MANAGER_LIVE=false

./bin/postgres_to_redshift
```
