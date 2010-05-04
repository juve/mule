import array

class BitSet(object):
	def __init__(self, size):
		self.bits = array.array('B',[0 for i in range((size+7)//8)] )
	
	def set(self, bit):
		b = self.bits[bit//8]
		self.bits[bit//8] = b | 1 << (bit % 8)
	
	def get(self, bit):
		b = self.bits[bit//8]
		return (b >> (bit % 8)) & 1

class f:
	def write(self, value):
		print "."

if __name__ == '__main__':
	import time
	start = time.time()
	b = BitSet(16*1024*8)
	for i in range(0, 100000):
		b.set(2)
	end = time.time()
	print "%f" % (end-start)
	#b.bits.tofile(open("bitset.dat","w"))
	import base64
	print base64.standard_b64encode(b.bits)

