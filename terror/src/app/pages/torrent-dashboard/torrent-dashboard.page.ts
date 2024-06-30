import { Component } from '@angular/core';
import { provideIcons } from '@ng-icons/core';
import { lucideListMusic, lucidePlusCircle, lucidePodcast } from '@ng-icons/lucide';

import { CommonModule, NgOptimizedImage } from '@angular/common';
import { HlmButtonDirective } from '@spartan-ng/ui-button-helm';
import { HlmCardDirective } from '@spartan-ng/ui-card-helm';
import { HlmIconComponent } from '@spartan-ng/ui-icon-helm';
import { BrnContextMenuTriggerDirective, BrnMenuTriggerDirective } from '@spartan-ng/ui-menu-brain';
import {
	HlmMenuComponent,
	HlmMenuGroupComponent,
	HlmMenuItemDirective,
	HlmMenuItemSubIndicatorComponent,
	HlmMenuSeparatorComponent,
	HlmSubMenuComponent,
} from '@spartan-ng/ui-menu-helm';
import { HlmScrollAreaComponent } from '@spartan-ng/ui-scrollarea-helm';
import { HlmSeparatorDirective } from '@spartan-ng/ui-separator-helm';
import {
	HlmTabsComponent,
	HlmTabsContentDirective,
	HlmTabsListComponent,
	HlmTabsTriggerDirective,
} from '@spartan-ng/ui-tabs-helm';
import { SideMusicMenuComponent } from 'src/app/components/side-menu/side-menu.component';
import { TorrentTableComponent } from 'src/app/components/torrent-table/torrent-table.component';


@Component({
	selector: 'spartan-music-example',
	standalone: true,
	host: {
		class: 'block',
	},
	imports: [
		TorrentTableComponent,
		HlmScrollAreaComponent,
		SideMusicMenuComponent,

		HlmTabsComponent,
		HlmTabsListComponent,
		HlmTabsTriggerDirective,
		HlmTabsContentDirective,

		HlmButtonDirective,
		HlmIconComponent,

		HlmSeparatorDirective,
		HlmScrollAreaComponent,

		BrnMenuTriggerDirective,
		BrnContextMenuTriggerDirective,
		HlmMenuComponent,
		HlmMenuGroupComponent,
		HlmMenuItemDirective,
		HlmSubMenuComponent,
		HlmMenuItemSubIndicatorComponent,
		HlmMenuSeparatorComponent,

		HlmCardDirective,
		NgOptimizedImage,
		CommonModule,
	],
	providers: [provideIcons({ lucidePlusCircle, lucideListMusic, lucidePodcast })],
	styles: `
		.fallback-img {
			filter: opacity(0.3);
		}
	`,
	templateUrl: `./torrent-dashboard.page.html`,
})
export default class TorrentDashboardPage {

	sectionData = {
		listenNow: [
			{
				img: 'https://images.pexels.com/photos/16580466/pexels-photo-16580466/free-photo-of-festa-comemoracao-musica-diversao.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=1',
				title: 'Angular Rendezvous',
				subtitle: 'Ethan Byte',
			},
			{
				img: 'https://images.pexels.com/photos/20548730/pexels-photo-20548730/free-photo-of-cidade-meio-urbano-homem-ponto-de-referencia.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'Async Awakenings',
				subtitle: 'Nina Netcode',
			},
			{
				img: 'https://images.pexels.com/photos/20555179/pexels-photo-20555179/free-photo-of-homem-sentado-jogando-musica.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'The Art of Reusability',
				subtitle: 'Lena Logic',
			},
			{
				img: 'https://images.pexels.com/photos/7365313/pexels-photo-7365313.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'Stateful Symphony',
				subtitle: 'Beth Binary',
			},
		],
		madeForYou: [
			{
				img: 'https://images.pexels.com/photos/20516167/pexels-photo-20516167/free-photo-of-preto-e-branco-p-b-mulher-relaxamento.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=1',
				title: 'Thinking Components',
				subtitle: 'Lena Logic',
			},
			{
				img: 'https://images.pexels.com/photos/4038323/pexels-photo-4038323.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'Functional Fury',
				subtitle: 'Beth Binary',
			},
			{
				img: 'https://images.pexels.com/photos/16580466/pexels-photo-16580466/free-photo-of-festa-comemoracao-musica-diversao.jpeg?auto=compress&cs=tinysrgb&w=1260&h=750&dpr=1',
				title: 'Angular Rendezvous',
				subtitle: 'Ethan Byte',
			},
			{
				img: 'https://images.pexels.com/photos/7365313/pexels-photo-7365313.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'Stateful Symphony',
				subtitle: 'Beth Binary',
			},
			{
				img: 'https://images.pexels.com/photos/20548730/pexels-photo-20548730/free-photo-of-cidade-meio-urbano-homem-ponto-de-referencia.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'Async Awakenings',
				subtitle: 'Nina Netcode',
			},
			{
				img: 'https://images.pexels.com/photos/20555179/pexels-photo-20555179/free-photo-of-homem-sentado-jogando-musica.jpeg?auto=compress&cs=tinysrgb&w=600',
				title: 'The Art of Reusability',
				subtitle: 'Lena Logic',
			},
		],
	};

	contextMenuPlaylist = [
		'Recently Added',
		'Recently Played',
		'Top Songs',
		'Top Albums',
		'Top Artists',
		'Logic Discography',
		'Bedtime Beats',
		'Feeling Happy',
		'I Miss Y2',
		'Runtober',
		'Mellow Days',
		'Eminem Essentials',
	];
}