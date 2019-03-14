# frozen_string_literal: true

require 'sequel'
require 'pg'
require 'logger'
require 'netrc'
require 'csv'
require 'chronic'

QUALTIY_CODES = { "E": '1',
                  " ": '4',
                  "G": '5',
                  "M": '2',
                  "Q": '3' }.freeze

def qualifier(flag)
  flag = 'G' if flag.empty?
  QUALTIY_CODES.fetch(flag.to_sym)
end

netrc = Netrc.read(Dir.home + '/.netrc.gpg')
credentials = netrc['database']

pr = Sequel.postgres(database: 'metadata',
                     loggers: [Logger.new($stdout)],
                     # host: '127.0.0.1',
                     # port: 5430,
                     host: 'granby.kbs.msu.edu',
                     user: credentials['login'],
                     password: credentials['password'])

sources = pr[Sequel.qualify(:odm, :sources)]
sites = pr[Sequel.qualify(:odm, :sites)]
variables = pr[Sequel.qualify(:odm, :variables)]
methods = pr[Sequel.qualify(:odm, :methods)]
mapping = pr[Sequel.qualify(:odm, :mapping)]

data = pr['select * from weather.kbs002_011 order by "Date"']

CSV.open('sources.csv', 'w') do |csv|
  csv << %w[Field SourceCode	Organization	SourceDescription	SourceLink	ContactName	Email	Citation]
  csv << [nil, sources.first.values].flatten
end

CSV.open('sites.csv', 'w') do |csv|
  csv << %w[Field	SiteCode	SiteName	Latitude	Longitude	LatLongDatumSRSName	SiteType	Comments]
  sites.each do |site|
    csv << [nil, site[:site_code], 'lter.kbs.' + site[:site_name], site[:latitude],
            site[:longitude], 'WSG84', 'Land', nil]
  end
end

CSV.open('variables.csv', 'w') do |csv|
  csv << %w[Field	VariableCode	VariableName	VariableUnitsName	DataType	SampleMedium	ValueType
            IsRegular	TimeSupport	TimeUnitsName	GeneralCategory	NoDataValue]
  variables.each do |var|
    csv << [nil, var[:variable_code], var[:variable_name], var[:variable_unit_name],
            var[:data_type], 'Air', 'Field Observation', 'TRUE', 1, 'hour', 'Climate', nil]
  end
end

CSV.open('method.csv', 'w') do |csv|
  csv << %w[Field	MethodCode	MethodDescription	MethodLink]
end

CSV.open('quality_control.csv', 'w') do |csv|
  csv << %w[Field	QualityControlLevelCode	Definition	Explanation]
  csv << [1, 'error', 'error value']
  csv << [2, 'missing', 'missing value']
  csv << [3, 'questionable]', 'questionable value']
  csv << [4, 'unkown]', 'unkown value']
  csv << [5, 'good', 'good value']
end

my_vars = variables.all
my_sites = sites.all
my_source = sources.first[:source_code]
my_mapping = mapping.all

CSV.open('data_values.csv', 'w') do |csv|
  csv << %w[Field DataValue LocalDateTime UTCOffset DateTimeUTC SiteCode VariableCode MethodCode
            SourceCode QualityControlLevelCode]

  data.each do |datum|
    datum.delete(:LTER_Site)
    date = datum.delete(:Date)
    station = datum.delete(:Station)

    flags, vars = datum.partition { |x, _y| x.to_s =~ /Flag/ }
    station_id = my_sites.first { |x| x[:site_name] == station.downcase }[:site_code]

    datetime = DateTime.new(date[0..3].to_i, date[4..5].to_i, date[6..7].to_i,
                            24, 0, 0, Rational(-5, 24))
    vars.each do |key, value|
      flag = flags.first { |x| x[:variable_name] == 'Flag_' + key }[1] || ''
      # variable_id = my_vars.first { |x| x[:variable_name] == key.to_s }[key.to_s]
      variable_id = my_mapping.first { |x| x[:variable_name] = key.to_s }[:variable_code]
      csv << [nil, value.to_s, datetime.to_s.tr('T', ' '), -5,
              datetime.new_offset(0).to_s.tr('T', ' '),
              station_id, variable_id, 0, my_source, qualifier(flag)]
    end
  end
end
