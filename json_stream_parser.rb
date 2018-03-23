require "JSON"
class JsonStreamParser

  BUFFER_SIZE   = 1024

  def initialize path
    raise ArgumentError unless File.exists?( path )

    @json_file           = File.open( path )
    @file_seek_offset   = 0 
    @stack_object       = 0
  end
  
  def get_block
    return nil unless @json_file.tell < @json_file.stat.size

    @json_file.seek(@file_seek_offset)
    @file_seek_offset += BUFFER_SIZE
    @json_file.read( BUFFER_SIZE )
  end


  def adjust_stack(letter)
    @stack_object += 1  if letter == '{'
    @stack_object -= 1  if letter == '}'  
  end 

  def to_json(json_string)
    # make hash from json string. Remove _id attributes as well
    return if json_string.nil?
    json_obj = JSON.parse(json_string).tap { |hs| hs.delete("_id") } 

    # Remove empty array
    json_obj = json_obj.tap { |hs| hs.delete("mentions") }  if json_obj.key?('mentions') && json_obj['mentions'].empty?
    json_obj = json_obj.tap { |hs| hs.delete("hashtags") }  if json_obj.key?('hashtags') && json_obj['hashtags'].empty?

    # remove non alpha numeric character from bio
    json_obj['bio'] = json_obj['bio'].gsub(/[^0-9a-z ]/i, '') if json_obj.key?('bio')

    json_obj
  end  

    
  def stream

    object_start_position = -1
    previous_data = ''

    while data = get_block

      index = 0
      data = previous_data + data

      data.split('').each { |c| 
        adjust_stack c
        object_start_position = index if @stack_object == 1 && c == '{'
        
        # yeild json if  object is completed
        yield to_json(data[object_start_position, (index - object_start_position) +1 ]) if @stack_object == 0 && c == '}' && object_start_position > -1 
       
        index += 1
      }

      if @stack_object == 1
        # current block has not been completed the opening json object. Keep the remaining string to be added in next block
        previous_data = data[object_start_position..-1]
        @stack_object       = 0

      else
        previous_data = ''
      end 
    end  
  end 
  
end  
