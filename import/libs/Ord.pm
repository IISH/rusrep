package Ord;

use vars qw(@ISA @EXPORT @EXPORT_OK %EXPORT_TAGS $VERSION);
use utf8;
use Encode;

use Exporter;

$VERSION = 1.00;
@ISA = qw(Exporter);

@EXPORT = qw(
		ordering
);

sub ordering
{
   my ($name, $DEBUG) = @_;

   if ($name)
   {
        $text = decode_utf8($name);
        if ($text=~/^\s*(\S)(\S)/)
        {
            $first = $1;
            $second = lc $2;
            $lc = lc $first;
            $up = uc $first;
            $ordID = ord($lc);
            $ordIDindex = ord($lc)*100000 + ord($second);
            my $thisitem = encode_utf8($text);
            print "$first $text $ordID\n" if ($DEBUG);
            $data{$text} = $ordID;
            $dataindex{$text} = $ordIDindex;
            $datachr{$text} = $up;
            $index{$up} = $ordID;
        }
    }

    return $ordIDindex;
};

