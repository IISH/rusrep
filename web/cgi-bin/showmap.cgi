#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

use utf8;
use Encode;
use DB_File;
use DBI;
use Configme;
$| = 1;

my %dbconfig = loadconfig("$Bin/../config/russianrep.config");
use Getopt::Long;

my $result = GetOptions(
    \%options,
    'csvfile=s' => \$csvfile,
    'all', 'help',
    'topic=s' => \$topic,
    'indicator=s' => \$indicator,
    'year=s' => \$year,
    'country=s' => \$country,
    'command=s' => \$command,
    'uri=s' => \$uri
    );

open(mapfile, "$Bin/map.template.html");
@map = <mapfile>;
close(mapfile);

$mapintro = $dbconfig{mapintro};
$mapintro_rus = $dbconfig{mapintro_rus};
if ($uri=~/\/ru\//)
{
   $mapintro = $mapintro_rus;
}
foreach $line (@map)
{
   $line=~s/\%\%mapintro\%\%/$mapintro/g;
   print "$line";
}
