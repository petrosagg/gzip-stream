_ = require 'lodash'
crcUtils = require 'resin-crc-utils'
CombinedStream = require 'combined-stream'
{ DeflateCRC32Stream } = require 'crc32-stream'

# gzip header
GZIP_HEADER = new Buffer([ 0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff ])

# DEFLATE ending block
DEFLATE_END = new Buffer([ 0x03, 0x00 ])

# Use the logic briefly described here by the author of zlib library:
# http://stackoverflow.com/questions/14744692/concatenate-multiple-zlib-compressed-data-streams-into-a-single-stream-efficient#comment51865187_14744792
# to generate deflate streams that can be concatenated into a gzip stream
class DeflatePartStream extends DeflateCRC32Stream
	constructor: ->
		@buf = new Buffer(0)
		super
	push: (chunk) ->
		if chunk isnt null
			if chunk.length >= 2
				# got another large enough chunk, previous chunk is safe to send
				super(@buf)
				@buf = chunk
			else
				@buf = Buffer.concat([@buf, chunk])
		else
			# got null signalling end of stream
			# inspect last chunk for 2-byte DEFLATE_END marker and remove it
			if @buf.length >= 2 and @buf[-2..].equals(DEFLATE_END)
				@buf = @buf[...-2]
			super(@buf)
			super(null)
	end: ->
		@flush =>
			super()
	metadata: ->
		crc: @digest()
		len: @size()
		zLen: @size(true)

exports.createDeflatePart = ->
	return new DeflatePartStream()

exports.createGzipFromParts = (parts) ->
	out = CombinedStream.create()
	# write the header
	out.append(GZIP_HEADER)
	# write all middle parts
	out.append(stream) for { stream } in parts
	# write ending DEFLATE part
	out.append(DEFLATE_END)
	# write CRC
	out.append(crcUtils.crc32_combine_multi(parts).combinedCrc32[0..3])
	# write length
	len = new Buffer(4)
	len.writeUInt32LE(_.sum(_.pluck(parts, 'len')), 0)
	out.append(len)
	# calculate compressed size. Add 10 byte header, 2 byte DEFLATE ending block, 8 byte footer
	out.zLen = _.sum(_.pluck(parts, 'zLen')) + 20
	# return stream
	return out
