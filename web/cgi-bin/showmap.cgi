#!/usr/bin/perl

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
use lib "$libpath/../libs";

open(mapfile, "$Bin/map.template.html");
@map = <mapfile>;
close(mapfile);

print "@map";
