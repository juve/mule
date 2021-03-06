import math
import array
import base64

# This is required because Condor doesn't allow ads larger 
# than 8192 chars in cronjob classads. Note: it takes x bytes
# to base64-encode .75x bytes, so this should give us 8000
# character chunks.
CHUNK_SIZE = (8000 * 3) / 4

class BitSet(object):
	def __init__(self, size):
		"""Create a bitset capable of storing 'size' bits"""
		self.bits = array.array('B',[0 for i in range((size+7)//8)] )
	
	def set(self, n):
		"""Set the nth bit in the set"""
		byte = self.bits[n//8]
		self.bits[n//8] = byte | 1 << (n % 8)
	
	def get(self, n):
		"""Get the value of the nth bit in the set"""
		byte = self.bits[n//8]
		return (byte >> (n % 8)) & 1
		
	def tobase64(self):
		"""Generate a base64-encoded copy of the bitset"""
		start = 0
		end = 0
		result = []
		while end < len(self.bits):
			end = min(len(self.bits), start + CHUNK_SIZE)
			result.append(base64.standard_b64encode(self.bits[start:end]))
			start = end
		return result

def hashpjw(s):
	"""A simple and reasonable string hash function due to Peter Weinberger"""
	val = 0
	for c in s:
		val = (val << 4) + ord(c)
		tmp = val & 0xf0000000
		if tmp != 0:
			val = val ^ (tmp >> 24)
			val = val ^ tmp
	return val

class BloomFilter(object):
	def __init__(self, m, k):
		"""Instantiate an m-bit Bloom filter using k hash indices per value"""
		self.m = m
		self.k = k
		self.bits = BitSet(size = self.m)

	def _hash(self, s):
		"""Hash s into k bit-vector indices for an m-bit Bloom filter"""
		indices = []
		h1 = hash(s)
		h2 = hashpjw(s)
		for i in xrange(1, self.k+1):
			indices.append((h1 + i*h2) % self.m)
		return indices

	def add(self, s):
		"""Insert s into the Bloom filter"""
		for i in self._hash(s): 
			self.bits.set(i)

	def contains(self, s):
		"""Return True if s is in the Bloom filter"""
		for i in self._hash(s):
			if self.bits.get(i) != 1:
				return False
		return True
		
	def tobase64(self):
		"""Generate a base64-encoded copy of the bloom filter"""
		return self.bits.tobase64()

if __name__ == '__main__':
	bs = BitSet(8)
	bs.set(1)
	print bs.get(1)
	print bs.get(7)
	bf = BloomFilter(65536, 1)
	for i in range(0,1000):
		bf.add("gideon_%d" % i)
	print bf.contains("gideon_0")
	print bf.contains("juve")
	print bf._hash("gideon")
	print bf.tobase64()

