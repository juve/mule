#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "classad_shared.h"

extern "C" {

/* decodes a base64-encoded character */
static int b64_value(unsigned char c)
{
	if(c >= 'A' && c <= 'Z')
		return c - 65;
	if(c >= 'a' && c <= 'z')
		return (c - 97) + 26;
	if(c >= '0' && c <= '9')
		return (c - 48) + 52;
	if(c == '+')
		return 62;
	if(c == '/')
		return 63;
	return c != '=' ? -1 : 0;
}

/* decodes a base64-encoded string */
static char * b64_decode(const char *s, int *n)
{
	int s_length = strlen(s);
	if (s_length == 0) return NULL;
	int i = 0;
	for(int j = s_length - 1; j > 0 && s[j] == '='; j--) {
		i++;
	}
	 
	int k = (s_length * 6) / 8 - i;
	*n = k;
	char *result = new char[k];
	int l = 0;
	for(int i1 = 0; i1 < s_length; i1 += 4) {
		int j1 = (b64_value(s[i1])     << 18) + 
				 (b64_value(s[i1 + 1]) << 12) + 
				 (b64_value(s[i1 + 2]) << 6) + 
				  b64_value(s[i1 + 3]);
		for(int k1 = 0; k1 < 3 && l + k1 < k; k1++) {
			result[l + k1] = (char)(j1 >> 8 * (2 - k1) & 0xff);
		}
		l += 3;
	}
	return result;
}

/* count the bits in a bit array */
static inline int popcount(char *buf, int n)
{
	int cnt=0;
	do {
		unsigned v = (unsigned)(*buf++);
		v = v - ((v >> 1) & 0x55555555);
		v = (v & 0x33333333) + ((v >> 2) & 0x33333333);
		cnt += (((v + (v >> 4)) & 0xF0F0F0F) * 0x1010101) >> 24;
	} while(--n);
	return cnt;
}

/* compares two bloom filters to see how many bits they have in common */
void bloom_compare(const int number_of_arguments,
				   const ClassAdSharedValue *arguments,
				   ClassAdSharedValue *result)
{
	int count = 0;

	if (number_of_arguments % 2 != 0) { 
		fprintf(stderr, "ERROR: invalid arguments: must be even number\n");
		result->type = ClassAdSharedType_Error;
		return;
	}

	int i;
	for (i=0; i<number_of_arguments; i+=2) {
		if (arguments[i].type != ClassAdSharedType_String ||
			arguments[i+1].type != ClassAdSharedType_String) {
			fprintf(stderr, "ERROR: invalid argument: expected string\n");
			result->type = ClassAdSharedType_Error;
			return;
		}

		int n1, n2;
		char *r1 = b64_decode(arguments[i].text, &n1);
		char *r2 = b64_decode(arguments[i+1].text, &n2);

		if (r1 == NULL || r2 == NULL) {
			fprintf(stderr, "ERROR: invalid argument\n");
			result->type = ClassAdSharedType_Error;
			if (r1) delete[] r1;
			if (r2) delete[] r2;
		}
		
		if (n1 != n2) {
			fprintf(stderr, "ERROR: bloom filters are different lengths\n");
			result->type = ClassAdSharedType_Error;
			delete [] r1;
			delete [] r2;
			return;
		}

		// AND the bits together
		for (int j=0; j<n1; j++) {
			r1[j] = r1[j] & r2[j];
		}

		// Count the # of bits in ANDed bitstring	
		count += popcount(r1, n1);

		delete [] r1;
		delete [] r2;
	}
	
	result->type = ClassAdSharedType_Integer;
	result->integer = count;

	return;
}

}
