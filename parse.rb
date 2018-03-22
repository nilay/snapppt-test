require_relative 'json_stream_parser'
jsp = JsonStreamParser.new '1000-users.json'

file_counter = index = 1
collections = []

jsp.stream do |data|
  collections << data
  if index % 10 == 0
    File.open("out#{file_counter}.json", 'w') {|f| f.write(collections.to_json) }
    collections = []
    file_counter += 1
  end 

  index += 1
end 

File.open("out#{file_counter}.json", 'w') {|f| f.write(collections.to_json) } if collections.count > 0
