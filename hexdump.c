#include <stdio.h>

int printable(unsigned char c)
{
  return (((c>=32) && (c<127)) || ((c>=160) && (c<=255)));
}

void hexdump(FILE *fd, unsigned char *buf, int n)
{
  int i,j,m;
  j=0;
  if (!n)
    return;
  if (n%16)
    m=n + 16 - (n % 16);
  else
    m=n;
  for (i=0; i<m; i++)
    {
      if (!(i%16))
	{
	  if (i>0)
	    {
	      fprintf(fd,"  ");
	      for (; j < i ; j++)
		{
		  if (j<n)
		    {
		      if (printable(buf[j]))
			fputc(buf[j],fd);
		      else
			fputc('.',fd);
		    }
		  else
		    fputc(' ',fd);
		}
	      if (i<n)
		fputc('\n',fd);
	    }
	  if (i<n)
	    fprintf(fd,"%04X: ",i);
	}
      if (i<n)
	fprintf(fd," %02X",(unsigned char)(buf[i]));
      else
	fprintf(fd,"   ");
    }
  fprintf(fd,"  ");
  for (; j < i ; j++)
    {
      if (j<n)
	{
	  if (printable(buf[j]))
	    fputc(buf[j],fd);
	  else
	    fputc('.',fd);
	}
      else
	fputc(' ',fd);
    }
  fputc('\n',fd);
  fflush(fd);
}

