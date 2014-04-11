#!/usr/bin/perl

while (<>)
{
    $str = $_;
    $str=~s/\r|\n//g;
    $str=~s/^.+?\(/\(/g;
    print "$str\n";
    if ($str=~/^(.+?)Values(.+)$/sxi)
    {
	my ($insert, $values) = ($1, $2);
	@fields = split(/\,\s+/, $insert);
	my $i;
	while ($values=~s/^\s*(.+?)\,\s+//)
	{
	    my $val = $1 || 'NULL';
	    print "$val $fields[$i]\n";
	    $i++;
	}

	print "$insert => $values\n";
    }
#    print "$str\n";
}
