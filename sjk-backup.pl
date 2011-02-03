#!/usr/bin/perl


use strict;
use warnings;

use Config::Scoped;
use File::Rsync;
use Parallel::ForkManager;

# Read the configuration file.
my $config = read_conf();

# Die if backup_root is not set up properly.
check_backup_root($config->{'general'}{'backup_root'});

# Do the backups.
do_backups($config);


################################################################################
#### FUNCTIONS
################################################################################

sub create_pid_file {
	my $pid = shift;

# Iterate through the hosts that should be backed up and execute the parallel
# rsyncs.
sub do_backups {
	my $config = shift;
	my $pfm = new Parallel::ForkManager(4); # Four rsyncs at a time seems good.
	foreach my $key (keys %{$config->{'backup'}}) {
		my $pid = $pfm->start and next; # Fork!
		#create_pid_file($pid, user, host); typ, eller något.
		backup_host($key, $config);
		$pfm->finish;
	}
	$pfm->wait_all_children;
}

sub backup_host {
	my ($name,$conf) = @_;
	my $hostconf = $conf->{'backup'}->{$name};

	my $user = $hostconf->{'user'};
	my $host = $hostconf->{'host'};
	my $backup_root = $conf->{'general'}->{'backup_root'};
	my $bwlimit = $hostconf->{'bwlimit'} ? $hostconf->{'bwlimit'} : 0;

#	print "Time to backup $name...\n";
#	foreach my $key (keys %{$conf}) {
#		print "$key = $conf->{$key}\n";
#	}

	foreach my $path (@{$hostconf->{'path'}}) {

		$path = strip_trailing_slash($path);
		my $src = "$user\@$host:\"$path\"";
		my $dst = "$backup_root/$name.0";

		print "BACKING UP $src TO $dst\n";
		my $rsync = File::Rsync->new({
			'archive'			=> 1,
			'hard-links'		=> 1,
			'human-readable'	=> 1,
			'inplace'			=> 1,
			'numeric-ids'		=> 1,
			'delete'			=> 1,
			'delete-excluded'	=> 1,
			'acls'				=> 1,
			#	'xattr'				=> 1,
			'partial'			=> 1,
			#	'progress'			=> 1,
			'verbose'			=> 1,
			'bwlimit'			=> $bwlimit,
		});
		$rsync->exec({
				src => $src,
				dest => $dst
		}) or warn "Rsync failed.";
	}
}

# Returns a reference to a structure holding the config data.
sub read_conf {
	my $parser = new Config::Scoped file => 'sjk-backup.conf';
	$config = $parser->parse;
	return $config;
}

sub check_backup_root {
	my $root = shift;
	die "No backup_root defined" unless defined $config->{'general'}{'backup_root'};
	die "$root does not exist" unless -e $root;
	die "$root is not writable by us" unless -w $root;
	die "$root is not a directory" unless -d $root;
}


# Read config file. 
#sub read_conf {
#	open my $fh, '<', 'sjk-backup.conf' or die "Couldn't open config: $!";
#
#	while(<$fh>) {
#		next if is_comment($_);
#		next if is_empty_line($_);
#
#		print "Config: $_";
#
#		if ($_ =~ /^backup_root (.*)$/) {
#			$config{'backup_root'} = $1;
#		}
#
#		if ($_ =~ /^backup ([^@]*)\@(.*):(.*) (.*)$/) {
#			print "Going to backup $1\@$2:$3 to $4\n";
#		}
#	}
#
#	close $fh or die "Couldn't close config: $!";
#
#	die "No backup_root defined." if not defined $config{'backup_root'};
#}

# is_comment($line)
# Returns true if $line is a comment, and false if it is not.
sub is_comment {
	my $line = shift;
	$line =~ s/^(?:\s|\t)+//;
	if ($line =~ /^#/) {
		return 1;
	} else {
		return 0;
	}
}

# is_empty_line($line)
# Returns true if $line is empty, false if it isn't
sub is_empty_line {
	my $line = shift;
	if ($line =~ /^(?:\s|\t)*$/) {
		return 1;
	} else {
		return 0;
	}
}

sub strip_trailing_slash {
	my $line = shift;

	# Don't do anything if $line is a local or remote root filesystem.
	return $line if ($line eq '/');
	return $line if ($line =~ m,:/$,);

	# Otherwise strip trailing slash.
	$line =~ s,/+$,,;

	return $line;
}
