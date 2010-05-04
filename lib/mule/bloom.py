import BitVector
import math

def hashpjw(s):
	"""A simple and reasonable string hash function due to Peter Weinberger."""
	val = 0
	for c in s:
		val = (val << 4) + ord(c)
		tmp = val & 0xf0000000
		if tmp != 0:
			val = val ^ (tmp >> 24)
			val = val ^ tmp
	return val

class BloomFilter:
	def __init__(self, m, k):
		"""Instantiate an m-bit Bloom filter using k hash indices per value."""
		self.n = 0
		self.m = m
		self.k = k
		self.bv = BitVector.BitVector(size = self.m)
		self.bits_in_inserted_values = 0

	def _HashIndices(self, s):
		"""Hash s into k bit-vector indices for an m-bit Bloom filter."""
		indices = []
		h1 = hash(s)
		h2 = hashpjw(s)
		for i in xrange(1, self.k+1):
			indices.append((h1 + i*h2) % self.m)
		return indices

	def Insert(self, s):
		"""Insert s into the Bloom filter."""
		for i in self._HashIndices(s): 
			self.bv[i] = 1
		self.n += 1
		self.bits_in_inserted_values += 8 * len(s)

	def InFilter(self, s):
		"""Return True if s is in the Bloom filter."""
		for i in self._HashIndices(s):
			if self.bv[i] != 1:
				return False
		return True

	def PrintStats(self):
		k = float(self.k)
		m = float(self.m)
		n = float(self.n)
		p_fp = math.pow(1.0 - math.exp(-(k * n) / m), k) * 100.0
		compression_ratio = float(self.bits_in_inserted_values) / m
		print "Number of filter bits (m) : %d" % self.m
		print "Number of filter elements (n) : %d" % self.n
		print "Number of filter hashes (k) : %d" % self.k
		print "Predicted false positive rate = %.2f" % p_fp
		print "Compression ratio = %.2f" % compression_ratio

def TestBloomFilter():
	import random
	holdback = set()
	bf = BloomFilter(131072, 3)
	f = open('words')
	for line in f:
		val = line.rstrip()
		if random.random() <= 0.10:
			holdback.add(val)
		else:
			bf.Insert(val)
	f.close()
	bf.PrintStats()
	num_false_positives = 0
	for val in holdback:
		if bf.InFilter(val):
			num_false_positives += 1
	rate = 100.0 * float(num_false_positives) / float(len(holdback))
	print "Actual false positive rate = %.2f%% (%d of %d)" % (rate, 
			num_false_positives, len(holdback))

if __name__ == '__main__':
	TestBloomFilter()

