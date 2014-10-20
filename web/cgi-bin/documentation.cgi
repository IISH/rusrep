#!/usr/bin/perl
#
# Copyright (C) 2014 International Institute of Social History.
#
# This program is free software: you can redistribute it and/or  modify
# it under the terms of the GNU Affero General Public License, version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# As a special exception, the copyright holders give permission to link the
# code of portions of this program with the OpenSSL library under certain
# conditions as described in each individual source file and distribute
# linked combinations including the program with the OpenSSL library. You
# must comply with the GNU Affero General Public License in all respects
# for all of the code used other than as permitted herein. If you modify
# file(s) with this exception, you may extend this exception to your
# version of the file(s), but you are not obligated to do so. If you do not
# wish to do so, delete this exception statement from your version. If you
# delete this exception statement from all source files in the program,
# then also delete it in the license file.

use vars qw/$libpath/;
use FindBin qw($Bin);
BEGIN { $libpath="$Bin" };
use lib "$libpath";
#use lib "$libpath/../libs";

use utf8;
use DBI;
use Configme;
my %dbconfig = loadconfig("$Bin/../config/russianrep.config");
$papersdir = $dbconfig{papersdir};
#print "Content-type: text/html\n\n";
@papers = find_papers($papersdir, $filter);

my ($dbname, $dbhost, $dblogin, $dbpassword) = ($dbconfig{dbname}, $dbconfig{dbhost}, $dbconfig{dblogin}, $dbconfig{dbpassword});
my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost",$dblogin,$dbpassword,{AutoCommit=>1,RaiseError=>1,PrintError=>0});

$topics = read_topics($dbh);

print $topics;

sub read_topics
{
    my ($dbh, $DEBUG) = @_;

    $sqlquery = "select topic_id, datatype, topic_name, description, topic_root, topic_name_rus from datasets.topics where 1=1";
    $sqlquery.=" and topic_id in ($topicIDs)" if ($topicIDs);
    $sqlquery.=" and topic_root in ($topicRoots)" if ($topicRoots);
    $sqlquery.=" and datatype in ($datatypes)" if ($datatypes=~/\d+/);
    $sqlquery.=" order by datatype asc";

    my $sth = $dbh->prepare("$sqlquery");
    $sth->execute();

    $html = "<table width=100% border=1>";
    while (my ($topic_id, $datatype, $topic_name, $description, $topic_root, $topic_name_rus) = $sth->fetchrow_array())
    {
	if ($datatype=~/\d+/)
	{ 
            $udatatype=$datatype;
            $udatatype=~s/\./\_/g;
	    $udatatype="ERRHS_".$udatatype; #."_\\d+";
	    my @papers;
	    #if ($topic_root || (!$topic_root && $datatype=~/^7/))
	    if ($datatype)
	    {
	        @papers = find_papers($papersdir, $udatatype);
	    }
	    else
	    {
		$root_datatype = $topic_name;
	    }
	    $tnames{$topic_root} = $topic_name if (!$topic_root);

	    if ($#papers >= 0 && !$topic_root)
	    {
#		$html.= "<tr><td colspan=\"3\">$datatype. $topic_name</td></tr>\n";
	    }

	    my @html;
	    foreach $file (@papers)
	    {
		$topic_root = $datatype unless ($topic_root);
		$html.= "<tr><td width=20%>$topic_root. $tnames{$topic_root}</td><td width=20%>$datatype. $topic_name</td><td width=60%><a href\=\"$uri{$file}\">$file</a></td></tr>\n";
#		$known{$file}++;
		#print "$paper\n";
	    }
	}
    }
    $html.="</table>";

    return $html;
}

sub find_papers
{
   my ($dir, $filter, $DEBUG) = @_;
   my @files;
   opendir(DIR, $dir) or die $!;

   my $uri = $dir;
   $uri=~s/^.+?public_html//g;
   while (my $file = readdir(DIR)) {
        if ($file=~/\.doc/ && (!$filter || $file=~/$filter/i) && $file=~/ERRHS/i)
        {
            push(@files, "$file");
	    $uri{"$file"} = "$uri/$file";
        }
   }
   closedir(DIR);

   return @files;
}
