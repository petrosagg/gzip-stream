_ = require 'lodash'
crcUtils = require 'crc-utils'
CombinedStream = require 'combined-stream'
{ DeflateCRC32Stream } = require 'crc32-stream'

# gzip header
GZIP_HEADER = new Buffer([ 0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff ])

# DEFLATE ending block
DEFLATE_END = new Buffer([ 0x03, 0x00 ])

exports.createDeflatePart = ->
	compress = new DeflateCRC32Stream()
	compress.end = ->
		compress.flush ->
			compress.emit('finish')
	compress.metadata = ->
		crc: @digest()
		len: @size()
		zLen: @size(true)
	return compress

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
